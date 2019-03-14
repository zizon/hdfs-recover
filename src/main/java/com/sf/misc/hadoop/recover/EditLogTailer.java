package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EditLogTailer {

    public static final Log LOGGER = LogFactory.getLog(EditLogTailer.class);

    protected final LogAggregator aggregator;
    protected final EditLogArchive archive;
    protected final Predicate<FSEditLogOp> op_filter;
    protected final Promise.PromiseFunction<FSEditLogOp, byte[]> line_serializer;
    protected final AtomicLong last_txid;

    public EditLogTailer(File log_storage, URI nameservice, String runas, Predicate<FSEditLogOp> op_filter, Promise.PromiseFunction<FSEditLogOp, byte[]> line_serializer) {
        this.aggregator = new LogAggregator(nameservice, runas);
        this.archive = new EditLogArchive(log_storage);
        this.op_filter = op_filter;
        this.line_serializer = line_serializer;
        this.last_txid = new AtomicLong(HdfsConstants.INVALID_TXID);
    }

    public Promise<?> start(long poll_period, long expiration_for_log) {
        LOGGER.info("start log tailing, poll_period:" + poll_period + " expiration_for_log:" + expiration_for_log);
        return Promise.period(
                () -> {
                    // start cleanup
                    Stream.of(
                            doClean(expiration_for_log),
                            doSync(poll_period)
                    ).collect(Promise.collector()).join();
                    return;
                },
                TimeUnit.MILLISECONDS.toMillis(poll_period)
        );
    }

    protected Promise<?> doClean(long expiration_for_log) {
        return archive.cleanExpireLogs(expiration_for_log);
    }

    protected Promise<?> doSync(long poll_period) {
        return archive.latest().lastTransactionID().transformAsync((txid) -> {
            txid = Math.max(txid, last_txid.get());
            LOGGER.info("start tailing... txid:" + txid);
            Promise<Long> updated = aggregator.skipUntil(txid).transform((iterator) -> {
                return LazyIterators.stream(iterator).parallel()
                        .filter(op_filter)
                        .map((op) -> {
                            try {
                                OutputStream stream = archive.streamForOp(op).join();
                                stream.write(line_serializer.apply(op));
                                stream.flush();
                            } catch (IOException e) {
                                last_txid.set(HdfsConstants.INVALID_TXID);
                                throw new UncheckedIOException("fail to write op:" + op, e);
                            }

                            return op.getTransactionId();
                        })
                        .max(Long::compareTo)
                        .orElse(HdfsConstants.INVALID_TXID);
            });

            final long start = txid;
            return updated.transform((last) -> {
                // update tracking
                last_txid.set(Math.max(last_txid.get(), last));

                LOGGER.info("end tailing...process:" + (last_txid.get() - start) + " trasactions, last txid:" + last_txid.get());
                return null;
            });
        });
    }
}
