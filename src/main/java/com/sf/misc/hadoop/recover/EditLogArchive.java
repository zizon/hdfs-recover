package com.sf.misc.hadoop.recover;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class EditLogArchive {

    public static final Log LOGGER = LogFactory.getLog(EditLogArchive.class);

    protected static final LoadingCache<File, OutputStream> STREAM_CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .removalListener((RemovalListener<File, OutputStream>) (notice) -> {
                Optional.ofNullable(notice.getValue()).ifPresent((stream) -> {
                    try {
                        stream.close();
                    } catch (IOException e) {
                        throw new UncheckedIOException("fail to close file:" + notice.getKey(), e);
                    }
                });
            })
            .build(new CacheLoader<File, OutputStream>() {
                @Override
                public OutputStream load(File key) throws Exception {
                    key.getParentFile().mkdirs();
                    return new FileOutputStream(key, true);
                }
            });

    protected static final String EDITLOG_PREFIX = "editlog-";


    public static class EditLogStat {

        protected final File file;

        protected final long timestamp;

        protected EditLogStat(File file) {
            this.file = file;
            this.timestamp = Long.valueOf(file.getName().replace(EDITLOG_PREFIX, ""));
        }

        public long timestamp() {
            return timestamp;
        }

        public File file() {
            return file;
        }

        public Promise<Long> lastTransactionID() {
            if (!file.exists()) {
                return Promise.success(HdfsConstants.INVALID_TXID);
            }

            return Promise.light(() -> {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    return reader.lines().parallel()
                            .map(RenameOldOpSerializer::deserialize)
                            .map(RenameOldOpSerializer.Rename::txid)
                            .max(Long::compareTo)
                            .orElse(HdfsConstants.INVALID_TXID);
                }
            });
        }
    }

    protected final File storage;

    public EditLogArchive(File storage) {
        this.storage = storage;
        storage.mkdirs();
        if (!storage.exists()) {
            throw new UncheckedIOException(new IOException("fail to cretae edit log storage:" + storage));
        }
    }

    public Promise<?> cleanExpireLogs(long expiration_for_log) {
        if (!storage.exists()) {
            return Promise.success(null);
        }

        long cut = System.currentTimeMillis() - expiration_for_log;
        return listEditLogs().parallelStream()
                .filter((state) -> cut >= state.timestamp())
                .map((stat) -> Promise.light(() -> stat.file().delete()))
                .collect(Promise.collector());
    }

    public Promise<OutputStream> streamForOp(FSEditLogOp op) {
        long timestamp = RenameOldOpSerializer.timestamp(op);

        // add prefix
        String name = EDITLOG_PREFIX
                + new SimpleDateFormat("yyyyMMddHH0000").format(new Date(timestamp));

        File candidate = new File(storage, name);
        return Promise.light(() -> STREAM_CACHE.get(candidate));
    }

    public File locateFileForTimestamp(long timestamp) {
        return new File(storage, EDITLOG_PREFIX + new SimpleDateFormat("yyyyMMddHH0000").format(new Date()));
    }

    public Collection<EditLogStat> listEditLogs() {
        try {
            return Files.list(storage.toPath()).parallel()
                    .map(Path::toFile)
                    .filter((file) -> file.getName().startsWith(EDITLOG_PREFIX))
                    .map(EditLogStat::new)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException("fail to list storage:" + storage, e);
        }
    }

    public EditLogStat latest() {
        return this.listEditLogs().parallelStream()
                .max(Comparator.comparing(EditLogStat::timestamp))
                .orElseGet(() -> new EditLogStat(this.locateFileForTimestamp(System.currentTimeMillis())));
    }
}