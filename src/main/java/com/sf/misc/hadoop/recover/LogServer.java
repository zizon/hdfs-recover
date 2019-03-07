package com.sf.misc.hadoop.recover;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LogServer implements Iterable<FSEditLogOp> {

    public static final Log LOGGER = LogFactory.getLog(LogServer.class);

    protected final InetSocketAddress address;
    protected final String journal_id;
    protected final NamespaceInfo namespace;

    public LogServer(InetSocketAddress address, String journal_id, NamespaceInfo namespace) {
        this.address = address;
        this.journal_id = journal_id;
        this.namespace = namespace;
    }

    protected Promise<QJournalProtocol> protocol(InetSocketAddress address) {
        return Promise.light(() -> {
            Configuration configuration = new Configuration(false);

            // recycle
            RPC.setProtocolEngine(configuration,
                    QJournalProtocolPB.class, ProtobufRpcEngine.class);
            QJournalProtocolPB pbproxy = RPC.getProxy(
                    QJournalProtocolPB.class,
                    RPC.getProtocolVersion(QJournalProtocolPB.class),
                    address, configuration);

            return new QJournalProtocolTranslatorPB(pbproxy);
        });
    }

    @Override
    public Iterator<FSEditLogOp> iterator() {
        Promise<QJournalProtocol> jouranl = protocol(address);

        // create segments
        Promise<Stream<LogSegment>> segments = jouranl.transform((protocol) -> {
            // touch jouran state
            QJournalProtocolProtos.GetEditLogManifestResponseProto response = protocol.getEditLogManifest(journal_id, 0, true);
            RemoteEditLogManifest manifest = PBHelper.convert(response.getManifest());

            // prepare fetch url
            URI fetch_server_template = URI.create(response.getFromURL());
            URI fetch_server = new URL(fetch_server_template.getScheme(), address.getHostName(), fetch_server_template.getPort(), "").toURI();

            return manifest.getLogs().parallelStream()
                    .map((editlog) -> {
                        return new LogSegment(fetch_server, journal_id, namespace, editlog.getStartTxId(), editlog.getEndTxId());
                    })
                    .map((segment) -> {
                        LOGGER.info(segment);
                        return segment;
                    })
                    ;
        });

        // add clean up
        segments.addListener(() -> {
            RPC.stopProxy(jouranl.join());
        }).logException();

        Promise<Iterator<FSEditLogOp>> may_be_duplicated = segments.transform((all) -> {
            return all.sorted(Comparator.comparing(LogSegment::from))
                    .flatMap((segment) -> {
                        return StreamSupport.stream(segment.spliterator(), false);
                    })
                    .sequential()
                    .iterator();
        });

        return new Iterator<FSEditLogOp>() {
            FSEditLogOp op;

            AtomicLong counter = new AtomicLong(0);

            @Override
            public boolean hasNext() {
                Iterator<FSEditLogOp> delegate = may_be_duplicated.join();
                if (!delegate.hasNext()) {
                    LOGGER.info("server transation:" + counter.get());
                    return false;
                }

                if (op == null) {
                    op = delegate.next();
                    counter.incrementAndGet();
                    return true;
                }

                // skip old
                FSEditLogOp current = null;
                for (current = delegate.next(); !current.hasTransactionId() || current.getTransactionId() < op.getTransactionId(); current = delegate.next()) {
                    if (!delegate.hasNext()) {
                        LOGGER.info("early stop server transation:" + counter.get());
                        return false;
                    }
                }
                op = current;

                counter.incrementAndGet();
                return true;
            }

            @Override
            public FSEditLogOp next() {
                return op;
            }
        };
    }

    @Override
    public String toString() {
        return "Server:" + address + " for Namespace:" + namespace + " journal:" + journal_id;
    }
}
