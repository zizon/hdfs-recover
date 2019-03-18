package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Collectors;

public class LogAggregator implements Iterable<FSEditLogOp> {

    public static final Log LOGGER = LogFactory.getLog(LogAggregator.class);

    protected final NamenodeRPC namenode;
    protected final boolean in_progress_ok;

    public LogAggregator(NamenodeRPC namenode, boolean in_progress_ok) {
        this.namenode = namenode;
        this.in_progress_ok = in_progress_ok;
    }

    public Promise<Iterator<FSEditLogOp>> skipUntil(long txid) {
        Promise<NamespaceInfo> namespace = namenode.protocol(NamenodeProtocol.class).transform(NamenodeProtocol::versionRequest);
        Promise<URI> jouranl_server = namenode.configuration()
                .transform((configuration) -> URI.create(configuration.get(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY)));

        // build server iterators
        return jouranl_server.transformAsync((journal) -> {
            String jouranl_id = journal.getPath().substring(1);
            return Arrays.stream(journal.getAuthority().split(";")).parallel()
                    .map((part) -> {
                        // trick to parse rpc server url
                        URI rpc = URI.create("any://" + part);

                        LOGGER.debug("create log server:" + rpc);
                        // closure to produce log server
                        return (Promise.PromiseFunction<NamespaceInfo, LogServer>) (info) -> new LogServer(
                                new InetSocketAddress(rpc.getHost(), rpc.getPort()),
                                jouranl_id,
                                info,
                                in_progress_ok
                        );
                    })
                    .map((log_server_genetator) -> {
                        return namespace.transformAsync((info) -> log_server_genetator.apply(info).skipUntil(txid));
                    })
                    .collect(Promise.listCollector())
                    .transform((iterators) -> {
                        return iterators.parallelStream()
                                .map(LazyIterators::prefetch)
                                .collect(Collectors.toList());
                    });
        }).transform((iterators) -> {
            return LazyIterators.merge(
                    Comparator.comparing(FSEditLogOp::getTransactionId),
                    iterators
            );
        });
    }

    @Override
    public Iterator<FSEditLogOp> iterator() {
        return skipUntil(HdfsConstants.INVALID_TXID).join();
    }
}
