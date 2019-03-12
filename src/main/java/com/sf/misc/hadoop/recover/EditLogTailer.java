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
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
        return Promise.period(
                () -> {
                    // start cleanup
                    Promise<?> clean_work = archive.cleanExpireLogs(expiration_for_log);

                    // append log
                    Promise<?> sync_log = archive.latest().lastTransactionID().transform((txid) -> {
                        txid = Math.max(txid, last_txid.get());
                        LOGGER.info("start tailing... txid:" + txid);
                        long updated = StreamSupport.stream(new Iterable<FSEditLogOp>() {
                            @Override
                            public Iterator<FSEditLogOp> iterator() {
                                return aggregator.iterator();
                            }
                        }.spliterator(), true).parallel()
                                .map((op) -> {
                                    if (op_filter.test(op)) {
                                        try {
                                            OutputStream stream = archive.streamForOp(op).join();
                                            stream.write(line_serializer.apply(op));
                                            stream.flush();
                                        } catch (IOException e) {
                                            throw new UncheckedIOException("fail to write op:" + op, e);
                                        }
                                    }

                                    return op.getTransactionId();
                                })
                                .max(Long::compareTo)
                                .orElse(HdfsConstants.INVALID_TXID);

                        last_txid.set(Math.max(last_txid.get(), updated));
                        LOGGER.info("end tailing, last txid:" + last_txid.get());
                        return null;
                    });

                    // join clean
                    Promise.all(clean_work, sync_log).join();
                    return;
                },
                TimeUnit.MILLISECONDS.toMillis(poll_period)
        );
    }
}
