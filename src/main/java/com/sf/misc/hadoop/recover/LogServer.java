package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Collectors;

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

    public InetSocketAddress rpcAddress() {
        return this.address;
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

    public Promise<Collection<LogSegment>> segments() {
        Promise<QJournalProtocol> jouranl = protocol(address);

        // create segments
        Promise<Collection<LogSegment>> segments = jouranl.transform((protocol) -> {
            // touch jouran state
            QJournalProtocolProtos.GetEditLogManifestResponseProto response = protocol.getEditLogManifest(journal_id, 0, true);
            RemoteEditLogManifest manifest = PBHelper.convert(response.getManifest());

            // prepare fetch url
            URI fetch_server_template = URI.create(response.getFromURL());
            URI fetch_server = new URL(fetch_server_template.getScheme(), address.getHostName(), fetch_server_template.getPort(), "").toURI();

            // sort by trasaction id
            return manifest.getLogs().parallelStream()
                    .map((editlog) -> {
                        return new LogSegment(fetch_server, journal_id, namespace, editlog.getStartTxId(), editlog.getEndTxId());
                    })
                    .sorted(Comparator.comparing(LogSegment::from))
                    .collect(Collectors.toList());
        });

        // add clean up
        segments.addListener(() -> {
            RPC.stopProxy(jouranl.join());
        }).logException();

        return segments;
    }

    public Promise<Iterator<FSEditLogOp>> skipUntil(long txid) {
        return segments().transform((segments) -> {
            return LazyIterators.stream(
                    LogSegment.merge(
                            segments.parallelStream()
                                    .filter((segment) -> segment.to() > txid)
                                    .collect(Collectors.toList())
                    )
            ).filter((op) -> op.getTransactionId() > txid).iterator();
        });
    }

    @Override
    public Iterator<FSEditLogOp> iterator() {
        return skipUntil(HdfsConstants.INVALID_TXID).join();
    }

    @Override
    public String toString() {
        return "Server:" + address + " for Namespace:" + namespace + " journal:" + journal_id;
    }
}
