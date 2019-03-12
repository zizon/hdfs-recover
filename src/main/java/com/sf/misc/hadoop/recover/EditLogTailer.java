package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

    public EditLogTailer(File log_storage, URI nameservice, String runas, Predicate<FSEditLogOp> op_filter, Promise.PromiseFunction<FSEditLogOp, byte[]> line_serializer) {
        this.aggregator = new LogAggregator(nameservice, runas);
        this.archive = new EditLogArchive(log_storage);
        this.op_filter = op_filter;
        this.line_serializer = line_serializer;
    }

    public Promise<?> start(long poll_period, long expiration_for_log) {
        return Promise.period(
                () -> {
                    // start cleanup
                    Promise<?> clean_work = archive.cleanExpireLogs(expiration_for_log);

                    LOGGER.info("start tailing...");
                    // append log
                    Promise<Long> last_txid = archive.latest().lastTransactionID();
                    StreamSupport.stream(aggregator.spliterator(), true).parallel()
                            .filter((op) -> {
                                return op.getTransactionId() > last_txid.join();
                            })
                            .filter(op_filter)
                            .forEach((op) -> {
                                try {
                                    OutputStream stream = archive.streamForOp(op).join();
                                    stream.write(line_serializer.apply(op));
                                    stream.flush();
                                } catch (IOException e) {
                                    throw new UncheckedIOException("fail to write op:" + op, e);
                                }
                            });
                    LOGGER.info("end tailing");

                    // join clean
                    clean_work.join();
                    return;
                },
                TimeUnit.MILLISECONDS.toMillis(poll_period)
        );
    }
}
