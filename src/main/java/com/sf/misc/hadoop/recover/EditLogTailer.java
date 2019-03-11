package com.sf.misc.hadoop.recover;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class EditLogTailer {

    public static final Log LOGGER = LogFactory.getLog(EditLogTailer.class);

    protected static final String SERVER_BASE_PREFIX = "editlog-";
    protected static final String ARCHIVE_PREFIX = "archive-";

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

    protected final LogAggregator aggregator;
    protected final File storage;
    protected final Predicate<FSEditLogOp> op_filter;

    public EditLogTailer(File log_storage, URI nameservice, String runas, Predicate<FSEditLogOp> op_filter) {
        this.aggregator = new LogAggregator(nameservice, runas);
        this.op_filter = op_filter;

        log_storage.mkdirs();
        if (!log_storage.exists()) {
            throw new UncheckedIOException(new IOException("fail to cretae edit log storage:" + log_storage));
        }
        this.storage = log_storage;
    }

    public Promise<?> start(long poll_period, long expiration_for_log) {
        return Promise.period(
                () -> {
                    this.aggregator.logServers()
                            .transformAsync((servers) -> {
                                return servers.parallelStream().map((server) -> {
                                    return server.transformAsync((resolved) -> scheduleForEachServer(resolved, expiration_for_log));
                                }).collect(Promise.collector());
                            }).logException().join();

                    return;
                },
                TimeUnit.MILLISECONDS.toMillis(poll_period)
        );
    }

    protected Promise<?> scheduleForEachServer(LogServer server, long expiration_for_log) {
        Promise<?> sync = Promise.light(() -> syncServer(server));
        Promise<?> cleanup = Promise.light(() -> cleanup(serverBaseDir(server), expiration_for_log));
        return Promise.all(sync, cleanup);
    }

    protected void cleanup(File base, long expiration_for_log) throws IOException {
        if (!base.exists()) {
            return;
        }

        long cut = System.currentTimeMillis() - expiration_for_log;
        // examine each file
        Files.list(base.toPath())
                .map(Path::toFile)
                .forEach((child) -> {
                    long archive_time_range = Long.valueOf(child.getName().replace(ARCHIVE_PREFIX, ""));
                    if (archive_time_range <= cut) {
                        LOGGER.info("clean up old file:" + child);
                        child.delete();
                    }
                });
    }

    protected void syncServer(LogServer server) {
        // filter rename
        long last_transation = estimateLastTransaction(server);
        LOGGER.info("sync server:" + server + " start txid:" + last_transation);
        StreamSupport.stream(server.spliterator(), true)
                .filter((op) -> {
                    if (!op.hasTransactionId()) {
                        return true;
                    } else {
                        return op.getTransactionId() > last_transation;
                    }
                })
                .filter(op_filter)
                .forEach((op) -> appendEditLog(server, op));
        LOGGER.info("sync server:" + server + " done");
        return;
    }

    protected void appendEditLog(LogServer server, FSEditLogOp op) {
        File base = serverBaseDir(server);
        OutputStream stream = locateStream(base, op);
        try {
            stream.write(serialize(op));
            stream.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("fail to append op:" + op + " for srever:" + server + " base dir:" + base, e);
        }
    }

    protected byte[] serialize(FSEditLogOp op) {
        return RenameOldOpSerializer.lineSerialize(op);
    }

    protected OutputStream locateStream(File base, FSEditLogOp op) {
        long timestamp = RenameOldOpSerializer.timestamp(op);

        // add prefix
        String name = ARCHIVE_PREFIX
                + new SimpleDateFormat("yyyyMMddHH0000").format(new Date(timestamp));

        File candidate = new File(base, name);
        return STREAM_CACHE.getUnchecked(candidate);
    }

    protected File serverBaseDir(LogServer server) {
        InetSocketAddress address = server.rpcAddress();
        String prefix = SERVER_BASE_PREFIX + address.getHostName() + "_" + address.getPort();

        return new File(storage, prefix);
    }

    protected long estimateLastTransaction(LogServer server) {
        File latest = findLatestFile(server);
        if (!latest.exists()) {
            return HdfsConstants.INVALID_TXID;
        }

        try {
            return new BufferedReader(new FileReader(latest)).lines().parallel()
                    .map(RenameOldOpSerializer::deserialize)
                    .map(RenameOldOpSerializer.Rename::txid)
                    .max(Long::compareTo)
                    .orElse(HdfsConstants.INVALID_TXID);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException("fail to find last timestamp", e);
        }
    }

    protected File findLatestFile(LogServer server) {
        File base = serverBaseDir(server);
        if (!base.exists()) {
            return new File(base, ARCHIVE_PREFIX + new SimpleDateFormat("yyyyMMddHH0000").format(new Date()));
        }

        try {
            return Files.list(base.toPath()).parallel()
                    .map(Path::toFile)
                    .filter((file) -> file.getName().startsWith(ARCHIVE_PREFIX))
                    .map((file) -> file.getName().replace(ARCHIVE_PREFIX, ""))
                    .map(Long::valueOf)
                    .max(Long::compareTo)
                    .map((time) -> {
                        return new File(base, ARCHIVE_PREFIX + time);
                    })
                    .orElseGet(() -> {
                        return new File(base, ARCHIVE_PREFIX + new SimpleDateFormat("yyyyMMddHH0000").format(new Date()));
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("fail to list file under:" + base, e);
        }
    }
}
