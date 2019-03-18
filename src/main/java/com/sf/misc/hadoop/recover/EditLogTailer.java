package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class EditLogTailer {

    public static final Log LOGGER = LogFactory.getLog(EditLogTailer.class);

    public static class FSOpStat {
        protected final FSEditLogOpCodes type;
        protected long counter;
        protected long start;
        protected long end;

        public FSOpStat(FSEditLogOpCodes type) {
            this.type = type;
            this.counter = 0;
            this.start = HdfsConstants.INVALID_TXID;
            this.end = HdfsConstants.INVALID_TXID;
        }

        public FSOpStat update(FSEditLogOp op) {
            this.counter += 1;
            if (this.start == HdfsConstants.INVALID_TXID) {
                this.start = op.getTransactionId();
            }
            if (this.end == HdfsConstants.INVALID_TXID) {
                this.end = op.getTransactionId();
            }

            this.end = Math.max(op.getTransactionId(), this.end);

            return this;
        }

        @Override
        public String toString() {
            return "op:" + type + " nums:" + counter + " start txid:" + start + " end:" + end;
        }
    }

    protected final LogAggregator aggregator;
    protected final EditLogArchive archive;
    protected final Predicate<FSEditLogOp> op_filter;
    protected final Promise.PromiseFunction<FSEditLogOp, byte[]> line_serializer;
    protected final AtomicLong last_txid;
    protected final Promise.PromiseConsumer<FSOpStat> stat_update_listener;
    protected ConcurrentMap<FSEditLogOpCodes, FSOpStat> stat;

    public EditLogTailer(File log_storage, NamenodeRPC namenode, Predicate<FSEditLogOp> op_filter, Promise.PromiseFunction<FSEditLogOp, byte[]> line_serializer, boolean in_progress_ok, Promise.PromiseConsumer<FSOpStat> stat_update_listener) {
        this.aggregator = new LogAggregator(namenode, in_progress_ok);
        this.archive = new EditLogArchive(log_storage);
        this.op_filter = op_filter;
        this.line_serializer = line_serializer;
        this.last_txid = new AtomicLong(HdfsConstants.INVALID_TXID);
        this.stat_update_listener = stat_update_listener;
        this.stat = new ConcurrentHashMap<>();
    }

    public Promise<?> start(long poll_period, long expiration_for_log) {
        LOGGER.info("start log tailing, poll_period:" + poll_period + " expiration_for_log:" + expiration_for_log);
        return Promise.period(
                () -> {
                    // fresh stat
                    ConcurrentMap<FSEditLogOpCodes, FSOpStat> new_stat = new ConcurrentHashMap<>();
                    // start cleanup
                    Stream.of(
                            doClean(expiration_for_log),
                            doSync(poll_period, new_stat)
                    ).collect(Promise.collector()).transform((ignore) -> {
                        // update stat
                        stat = new_stat;

                        // notify
                        new_stat.values().parallelStream()
                                .filter((stat) -> stat.start != HdfsConstants.INVALID_TXID)
                                .forEach(stat_update_listener);

                        return null;
                    }).join();
                    return;
                },
                TimeUnit.MILLISECONDS.toMillis(poll_period)
        );
    }

    public ConcurrentMap<FSEditLogOpCodes, FSOpStat> stat() {
        return this.stat;
    }

    protected Promise<Void> doClean(long expiration_for_log) {
        return archive.cleanExpireLogs(expiration_for_log);
    }

    protected Promise<Void> doSync(long poll_period, ConcurrentMap<FSEditLogOpCodes, FSOpStat> stating) {
        return archive.latest().lastTransactionID().transformAsync((txid) -> {
            final long start_txid = Math.max(txid, last_txid.get());
            LOGGER.info("start tailing... txid:" + start_txid);
            Promise<Long> updated = aggregator.skipUntil(start_txid).transform((iterator) -> {
                return LazyIterators.stream(iterator).parallel()
                        .map((op) -> {
                            // update stat
                            stating.compute(op.opCode, (type, value) -> {
                                return Optional.ofNullable(value)
                                        .orElseGet(() -> new FSOpStat(type))
                                        .update(op)
                                        ;
                            });

                            return op;
                        })
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

            return updated.transform((last) -> {
                // update tracking
                last_txid.set(Math.max(start_txid, last));

                LOGGER.info("end tailing...process:" + (last_txid.get() - start_txid) + " trasactions, last txid:" + last_txid.get());
                return null;
            });
        });
    }
}
