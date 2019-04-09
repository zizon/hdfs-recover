package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ReverseReplay {

    public static final Log LOGGER = LogFactory.getLog(ReverseReplay.class);

    protected final EditLogArchive archive;
    protected final FileSystem fs;
    protected final ResolvableRecover.RecoverInterceptor interceptor;

    public ReverseReplay(EditLogArchive archive, FileSystem fs, ResolvableRecover.RecoverInterceptor interceptor) {
        this.archive = archive;
        this.interceptor = interceptor;
        this.fs = fs;
    }

    public Promise<Boolean> forRange(long from, long to) {
        // time format are in 20190318000000,not unix timestamp
        // trim HHmmss
        final long wrap_part = 1000000;
        long wrap_from = from - from % wrap_part;
        long wrap_to = to - to % wrap_part;

        // find ops
        Iterator<RenameOldOpSerializer.Rename> ops = LazyIterators.concat(
                this.archive.listEditLogs().parallelStream()
                        // filter editlogs that in range
                        .filter((state) -> state.timestamp() >= wrap_from && state.timestamp() <= wrap_to)
                        .map((state) -> {
                            LOGGER.info("selected editlog:" + state);
                            return (Promise.PromiseSupplier<Iterator<RenameOldOpSerializer.Rename>>) () -> iterate(state);
                        })
        );

        // then filter
        return LazyIterators.stream(ops).filter((op) -> {
            return op.timestamp() >= from && op.timestamp() <= to;
        }).parallel().map((op) -> {
            return reverse(op).transform((ok) -> {
                if (!ok) {
                    interceptor.onRenameFail(new WriteEditLogOP(
                            FSEditLogOpCodes.OP_RENAME_OLD,
                            op.txid(),
                            op.timestamp(),
                            new Path(op.source()),
                            new Path(op.target())
                    ), null);
                }

                interceptor.onRenameOk(new WriteEditLogOP(
                        FSEditLogOpCodes.OP_RENAME_OLD,
                        op.txid(),
                        op.timestamp(),
                        new Path(op.source()),
                        new Path(op.target())
                ));
                return null;
            }).catching((throwable) -> {
                interceptor.onRenameFail(new WriteEditLogOP(
                        FSEditLogOpCodes.OP_RENAME_OLD,
                        op.txid(),
                        op.timestamp(),
                        new Path(op.source()),
                        new Path(op.target())
                ), throwable);
            });
        }).collect(Promise.collector())
                .transform((ignore) -> true);
    }

    protected Promise<Boolean> move(Path rename_source, Path rename_target) {
        return fsRename(rename_source, rename_target)
                .transformAsync((ok) -> {
                    if (ok) {
                        // lucky, fast path
                        LOGGER.info("rename source:" + rename_source + " to:" + rename_target + " ok");
                        return Promise.success(true);
                    }

                    if (!fs.exists(rename_source)) {
                        // source not found,but assume ok
                        LOGGER.warn("no source found:" + rename_source + " assume operation ok");
                        return Promise.success(true);
                    }

                    if (fs.exists(rename_target)) {
                        // target already exists,resolve conflict
                        return resolveMoveConflict(rename_source, rename_target);
                    }

                    // source exits,and target not exists,
                    return preserveParent(rename_source, rename_target)
                            .transformAsync((clone_ok) -> {
                                if (!clone_ok) {
                                    LOGGER.warn("fail to preserve directory of target:" + rename_target + " source:" + rename_source);
                                    return Promise.success(false);

                                }

                                return fsRename(rename_source, rename_target)
                                        .transform((move_ok) -> {
                                            if (move_ok) {
                                                LOGGER.info("move source:" + rename_source + " to:" + rename_target + " ok");
                                                return true;
                                            }

                                            // move fail
                                            if (!fs.exists(rename_source)) {
                                                // source
                                                LOGGER.info("source may had been move:" + rename_source + " assume ok");
                                                return true;
                                            }

                                            LOGGER.warn("fail to move:" + rename_source + " target");
                                            return false;
                                        });
                            });
                });
    }

    protected Promise<Boolean> reverse(RenameOldOpSerializer.Rename op) {
        // note,swap the source and target
        Path rename_source = interceptor.transformSource(new Path(op.target()));
        Path rename_target = interceptor.transformTarget(new Path(op.source()));

        return move(rename_source, rename_target);
    }

    protected Promise<Boolean> resolveMoveConflict(Path source, Path target) {
        Promise<FileStatus> source_status = Promise.light(() -> fs.getFileStatus(source));
        Promise<FileStatus> target_status = Promise.light(() -> fs.getFileStatus(target)).fallback(() -> {
            // exception occurs,may be some one had move,try latter
            return fs.getFileStatus(target);
        });

        return Promise.all(source_status, target_status)
                .transformAsync((ignore) -> {
                    FileStatus source_file = source_status.join();
                    FileStatus target_file = target_status.join();

                    // source is newwer
                    if (source_file.getModificationTime() > target_file.getModificationTime()) {
                        Path conflict_resolved_target = new Path(target.getParent(), ".conflict-" + UUID.randomUUID() + "." + target.getName());
                        LOGGER.info("source:" + source_file + " is newer than target:" + target_file + " ,try rename target to:" + conflict_resolved_target);

                        return move(target, conflict_resolved_target)
                                .transformAsync((ok) -> {
                                    if (ok) {
                                        // target backup ok
                                        return move(source, target);
                                    }

                                    // target rename fail
                                    LOGGER.warn("try move conflict target:" + target + " to:" + conflict_resolved_target + " fail");
                                    return Promise.success(false);
                                });
                    }

                    LOGGER.info("source:" + source + " is older than target:" + target + ", do nothing");
                    // source is older,do nothing
                    return Promise.success(true);
                }).catching((throwable) -> {
                    Promise.success(true);
                });
    }

    protected Promise<Boolean> preserveParent(Path source, Path target) {
        return Promise.light(() -> fs.exists(target.getParent()))
                .transformAsync((exists) -> {
                    if (exists) {
                        return Promise.success(true);
                    }

                    // target parent not exists,ensure grand parent
                    return preserveParent(source.getParent(), target.getParent())
                            .transform((grand_ok) -> {
                                if (!grand_ok) {
                                    LOGGER.warn("can not perserve parent of target:" + target);
                                    return false;
                                }

                                // grand parent ok
                                FileStatus source_parent = fs.getFileStatus(source.getParent());

                                // clone permission
                                fs.mkdirs(target.getParent(), source_parent.getPermission());
                                return true;
                            });
                });
    }

    protected Promise<Boolean> fsRename(Path source, Path target) {
        if (interceptor.dryrun()) {
            return Promise.success(true);
        }

        return Promise.light(() -> fs.rename(source, target));
    }

    protected Iterator<RenameOldOpSerializer.Rename> iterate(EditLogArchive.EditLogStat stat) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(stat.file()));
            Iterator<RenameOldOpSerializer.Rename> iterator = reader.lines().parallel()
                    .map(RenameOldOpSerializer::deserialize)
                    .iterator();

            return new Iterator<RenameOldOpSerializer.Rename>() {
                @Override
                public boolean hasNext() {
                    boolean more = iterator.hasNext();
                    if (more) {
                        return true;
                    }

                    // release resource
                    Promise.delay(reader::close, TimeUnit.SECONDS.toMillis(5))
                            .logException();
                    return false;
                }

                @Override
                public RenameOldOpSerializer.Rename next() {
                    return iterator.next();
                }
            };
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException("no file found:" + stat.file(), e);
        }
    }

    public static void main(String[] args) throws ParseException {
        if (args.length != 4) {
            System.out.println("example: \n"
                    + "dryrun: java " + ReverseReplay.class.getName() + " test-cluster://10.202.77.200:8020,10.202.77.201:8020 false 20190318000000 20190318201532 \n"
                    + "move: java " + ReverseReplay.class.getName() + " test-cluster://10.202.77.200:8020,10.202.77.201:8020 true 20190318000000 20190318201532 \n"
            );

            return;
        }

        File storage = new File("__storage__");
        URI nameservice = URI.create(args[0]);
        boolean dryrun = !Boolean.parseBoolean(args[1]);

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        long from = format.parse(args[2]).getTime();
        long to = format.parse(args[3]).getTime();

        ResolvableRecover.RecoverInterceptor interceptor = new ResolvableRecover.RecoverInterceptor() {
            @Override
            public boolean dryrun() {
                return dryrun;
            }

            @Override
            public void onRenameFail(WriteEditLogOP op, Throwable reason) {
                System.out.println("replay fail of op: " + op);
            }

            @Override
            public void onRenameOk(WriteEditLogOP op) {
                System.out.println("replay ok of op: " + op);
            }
        };

        NamenodeRPC namendoe = new NamenodeRPC(nameservice, "hdfs");
        LogAggregator aggregator = new LogAggregator(namendoe, true);
        EditLogArchive archive = new EditLogArchive(storage);

        Promise<?> tailing = new EditLogTailer(
                archive,
                aggregator,
                (op) -> op.opCode.compareTo(FSEditLogOpCodes.OP_RENAME_OLD) == 0,
                RenameOldOpSerializer::lineSerialize,
                (stat) -> {
                    //LOGGER.info("stat:" + stat);
                }
        ).start(1, TimeUnit.DAYS.toMillis(365));


        namendoe.fs().transform((fs) -> {
            return new ResolvableRecover(
                    aggregator,
                    archive,
                    interceptor,
                    fs
            );
        }).transformAsync((recover) -> {
            return recover.replay(from, to);
        }).join();

        LOGGER.info("done");
        tailing.cancel(true);
        tailing.maybe();
        LOGGER.info("all finished");

        System.exit(0);
    }
}
