package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

public class ResolvableRecover {

    public static final Log LOGGER = LogFactory.getLog(ResolvableRecover.class);

    public interface RecoverInterceptor {

        default boolean dryrun() {
            return true;
        }

        default Path transformSource(Path source) {
            return source;
        }

        default Path transformTarget(Path target) {
            return target;
        }

        default void onRenameFail(WriteEditLogOP op, Throwable reason) {
        }

        default void onRenameOk(WriteEditLogOP op) {
        }
    }

    protected final NavigableSet<WriteEditLogOP> ops;
    protected final RecoverInterceptor interceptor;
    protected final FileSystem fs;
    protected final DFSClient client;
    protected boolean terminated;


    public ResolvableRecover(LogAggregator aggregator, EditLogArchive archive, RecoverInterceptor interceptor, FileSystem fs, DFSClient client) {
        this.terminated = false;
        this.interceptor = interceptor;
        this.fs = fs;
        this.client = client;
        this.ops = new ConcurrentSkipListSet<>(Comparator.comparing(WriteEditLogOP::txid));
        loadEditlogs(archive);
        starPolling(aggregator);
    }

    public Promise<Void> replay(long start, long end) {
        return this.ops.parallelStream() //
                .filter((op) -> op.timestamp() >= start && op.timestamp() <= end) //
                .filter((op) -> op.type().compareTo(FSEditLogOpCodes.OP_RENAME_OLD) == 0) //
                .filter((op) -> !op.from().toUri().getPath().contains(".Trash")) //
                .filter((op) -> op.to().toUri().getPath().contains(".Trash")) //
                .map(this::moveOutOfTrash)
                .collect(Promise.listCollector())
                .transform((result) -> {
                    result.parallelStream()
                            .forEach(Promise.PromiseRunnable::run);
                    return null;
                });
    }

    protected Promise<Promise.PromiseRunnable> moveOutOfTrash(WriteEditLogOP op) {
        return move(
                interceptor.transformSource(op.to()),
                interceptor.transformTarget(op.from()),
                op.txid()
        ).<Promise.PromiseRunnable>transform((ok) -> {
            if (ok) {
                return () -> interceptor.onRenameOk(op);
            } else {
                return () -> interceptor.onRenameFail(op, null);
            }
        }).fallback((throwable) -> {
            return () -> interceptor.onRenameFail(op, throwable);
        });
    }

    protected boolean rename(Path source, Path target) {
        try {
            client.rename(source.toString(), target.toString(), Options.Rename.NONE);
            return true;
        } catch (IOException exists) {
            return false;
        }
    }

    protected Promise<Boolean> move(Path source, Path target, long txid) {
        return Promise.light(() -> {
            LOGGER.info("try move:" + source + " to target:" + target);
            if (interceptor.dryrun()) {
                return Promise.success(true);
            }

            // try direct move,use rename2 api
            if (rename(source, target)) {
                LOGGER.info("reanme source:" + source + " to target:" + target + " ok");
                return Promise.success(true);
            }

            if (!fs.exists(source)) {
                // source may have been move,try find current
                Promise<Optional<Map.Entry<Long, Path>>> current = findCurrent(txid, source);
                return current.transformAsync((entry) -> {
                    if (entry.isPresent()) {
                        return move(entry.get().getValue(), target, entry.get().getKey());
                    }

                    return Promise.success(true);
                });
            }


            // source exits,and see if target exits?
            if (fs.exists(target)) {
                // resolve conflit?
                LOGGER.info("conlift? source:" + source + " target:" + target);
                return resolveConflict(source, target, txid);
            }

            // then preserve directory and try agtain
            return preserveDirecotry(source, target)
                    .transformAsync((ok) -> {
                        if (!ok) {
                            return Promise.success(false);
                        }

                        return move(source, target, txid);
                    });
        }).transformAsync((through) -> through);
    }

    protected Promise<Optional<Map.Entry<Long, Path>>> findCurrent(long txid, Path path) {
        return Promise.light(() -> ops.parallelStream().filter((op) -> op.txid() > txid)
                .filter((op) -> {
                    String op_path = op.from().toUri().getPath();
                    String raw_path = path.toUri().getPath();
                    return raw_path.startsWith(op_path);
                })
                .max(Comparator.comparing(WriteEditLogOP::txid))
                .map((op) -> {
                    String op_path = op.from().toUri().getPath();
                    String raw_path = path.toUri().getPath();
                    Path new_path = new Path(raw_path.replace(op_path, op.to().toUri().getPath()));
                    return new AbstractMap.SimpleImmutableEntry<>(op.txid(), new_path);
                })
        );
    }

    protected Promise<Boolean> resolveConflict(Path source, Path target, long txid) {
        if (source.equals(target)) {
            // special case,when invoke in a findCurrent context
            return Promise.success(true);
        }

        Promise<FileStatus> source_status = Promise.light(() -> fs.getFileStatus(source));
        Promise<FileStatus> target_status = Promise.light(() -> fs.getFileStatus(target));

        return Promise.all(source_status, target_status)
                .transformAsync((ignore) -> {
                    FileStatus from = source_status.join();
                    FileStatus to = target_status.join();

                    if (to.getModificationTime() > from.getModificationTime()) {
                        // target newwer,assume ok
                        Path conlift_resolved = new Path(target.getParent(), ".conflicted." + UUID.randomUUID() + "." + target.getName());
                        this.rename(source, conlift_resolved);
                        LOGGER.info("target is newwer,give up source:" + source + " target:" + target + " resolved:" + conlift_resolved);
                        return Promise.success(true);
                    }

                    // source newer
                    Path conflict_resovled = new Path(target.getParent(), ".conflicted." + UUID.randomUUID() + "." + target.getName());
                    // make target as conflicted
                    if (this.rename(target, conflict_resovled)) {
                        LOGGER.info("should tabke over,reanme target:" + target + " to:" + conflict_resovled + " source:" + source);
                        // target *SHOULD* had bean move,
                        // try move source to target as accomplished.
                        if (this.rename(source, target)) {
                            // fine,resolve
                            LOGGER.info("take over ok, source:" + source + " target:" + target);
                            return Promise.success(true);
                        }

                        //  target status change,may pointing to another file,
                        // try from scratch
                        LOGGER.info("take over fail,souce:" + source + " target:" + target + " try again");
                        return move(source, target, txid);
                    }

                    // maybe target not exists,or we should had succeed
                    LOGGER.info("take over fail,try rename directryly,source:" + source + " target:" + target);
                    if (this.rename(source, target)) {
                        // fine
                        LOGGER.info("after take over fail,rename:" + source + " to :" + target);
                        return Promise.success(true);
                    }

                    // or,fail to move source to target,
                    // maybe something had change,try from scratch.
                    LOGGER.info("no ideal,simpel try again,source:" + source + " target:" + target);
                    return move(source, target, txid);
                });
    }

    protected Promise<Boolean> preserveDirecotry(Path source, Path target) {
        return Promise.light(() -> {
            Path target_parent = target.getParent();
            if (target_parent == null || fs.exists(target_parent)) {
                return Promise.success(true);
            }

            // target parent not exitst
            return preserveDirecotry(source.getParent(), target.getParent())
                    .transform((grand_parent) -> {
                        if (!grand_parent) {
                            return false;
                        }

                        // grand parent ok
                        FileStatus status = fs.getFileStatus(source.getParent());
                        if (status == null) {
                            return false;
                        }

                        // then copy parent
                        fs.mkdirs(target.getParent(), status.getPermission());
                        return fs.exists(target.getParent());
                    });

        }).transformAsync((through) -> through);
    }

    protected void loadEditlogs(EditLogArchive archive) {
        Iterator<RenameOldOpSerializer.Rename> iterator = LazyIterators.concat(
                archive.listEditLogs().parallelStream()
                        .map((state) -> (Promise.PromiseSupplier<Iterator<RenameOldOpSerializer.Rename>>) () -> iterate(state))
        );

        LazyIterators.stream(iterator)
                .map((op) -> new WriteEditLogOP(
                                FSEditLogOpCodes.OP_RENAME_OLD,
                                op.txid(),
                                op.timestamp(),
                                new Path(op.source()),
                                new Path(op.target())
                        )
                )
                .forEach(ops::add);

        return;
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

    protected void starPolling(LogAggregator aggregator) {
        Promise cancled = Promise.promise();

        long last = ops.isEmpty() ? HdfsConstants.INVALID_TXID : ops.last().txid();
        pollOnce(aggregator, last);

        Promise<?> period = Promise.period(() -> {
            if (terminated) {
                cancled.cancel(true);
                return;
            }

            pollOnce(aggregator, ops.last().txid());
        }, TimeUnit.MINUTES.toMillis(1)).logException();

        // chain
        cancled.addListener(() -> period.cancel(true));
    }

    protected void pollOnce(LogAggregator aggregator, long txid) {
        aggregator.skipUntil(txid).maybe().ifPresent((iterator) -> {
            LazyIterators.stream(iterator).parallel()
                    .filter(WriteEditLogOP::accept)
                    .map(WriteEditLogOP::new)
                    .forEach(ops::add);
        });

        return;
    }

}
