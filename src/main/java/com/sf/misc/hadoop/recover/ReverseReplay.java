package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ReverseReplay {

    protected static final String PATH_SEPERATOR = Path.SEPARATOR;

    public static final Log LOGGER = LogFactory.getLog(ReverseReplay.class);

    protected final EditLogArchive archive;
    protected final FileSystem fs;
    protected final RenameInterceptor interceptor;

    public interface RenameInterceptor {

        default boolean dryrun() {
            return true;
        }

        default Path transformSource(Path source) {
            return source;
        }

        default Path transformTarget(Path target) {
            return target;
        }

        default void onRenameFail(RenameOldOpSerializer.Rename op, Throwable reason) {
            //LOGGER.error("rename fail:" + op, reason);
        }
    }

    public ReverseReplay(EditLogArchive archive, FileSystem fs, RenameInterceptor interceptor) {
        this.archive = archive;
        this.interceptor = interceptor;
        this.fs = fs;
    }

    public Promise<Boolean> forRange(long from, long to) {
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
                    interceptor.onRenameFail(op, null);
                }
                return null;
            }).catching((throwable) -> {
                interceptor.onRenameFail(op, throwable);
            });
        }).collect(Promise.collector())
                .transform((ignore) -> true);
    }

    protected Promise<Boolean> reverse(RenameOldOpSerializer.Rename op) {
        // note,swap the source and target
        Path rename_source = interceptor.transformSource(new Path(op.target()));
        Path rename_target = interceptor.transformTarget(new Path(op.source()));

        //LOGGER.info("reverse:" + rename_source + " target:" + rename_target);
        return doRename(rename_source, rename_target)
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
                        return resolveConflict(rename_source, rename_target);
                    }

                    LOGGER.info("try create:" + rename_target);
                    // source exits,and target not exists,
                    return cloneDirectory(rename_source, rename_target)
                            .transformAsync((clone_ok) -> {
                                if (clone_ok) {
                                    return doRename(rename_source, rename_target);
                                }

                                return Promise.success(false);
                            });
                });
    }

    protected Promise<Boolean> resolveConflict(Path source, Path target) {
        Promise<FileStatus> source_status = Promise.light(() -> fs.getFileStatus(source));
        Promise<FileStatus> target_status = Promise.light(() -> fs.getFileStatus(target));

        return Promise.all(source_status, target_status)
                .transformAsync((ignore) -> {
                    FileStatus source_file = source_status.join();
                    FileStatus target_file = target_status.join();

                    if (source_file.getModificationTime() > target_file.getModificationTime()) {
                        Path conflict_resolved_target = new Path(target.getParent(), ".conflict." + target.getName());
                        LOGGER.info("source:" + source_file + " is newer than target:" + target_file + " ,try rename target to:" + conflict_resolved_target);

                        return doRename(target, conflict_resolved_target)
                                .transformAsync((ok) -> {
                                    if (ok) {
                                        return doRename(source, target);
                                    }

                                    LOGGER.warn("try resolve conflict:" + target + " (rename to:" + conflict_resolved_target + ") but failed");
                                    return Promise.success(false);
                                });
                    }

                    // sourfile is older,do nothing
                    return Promise.success(true);
                });
    }

    protected Promise<Boolean> cloneDirectory(Path source, Path target) {
        return Promise.light(() -> fs.exists(target.getParent()))
                .transformAsync((exists) -> {
                    if (!exists) {
                        // parent not exists
                        return cloneDirectory(source.getParent(), target.getParent());
                    }

                    return Promise.success(true);
                })
                .transformAsync((parent_ok) -> {
                    if (!parent_ok) {
                        return Promise.success(false);
                    }

                    if (fs.isDirectory(source)) {
                        FileStatus status = fs.getFileStatus(source);

                        // clone permission
                        fs.mkdirs(target, status.getPermission());
                    }

                    return Promise.success(true);
                });
    }

    protected Promise<Boolean> doRename(Path source, Path target) {
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
}
