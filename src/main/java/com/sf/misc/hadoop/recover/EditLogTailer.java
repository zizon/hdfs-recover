package com.sf.misc.hadoop.recover;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.qjournal.server.GetJournalEditServlet;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class EditLogTailer {

    public static final Log LOGGER = LogFactory.getLog(EditLogTailer.class);

    protected final String journal_id;
    protected final URLConnectionFactory factory;

    protected final NamenodeProtocol namenode;
    protected final Configuration configuration;
    protected final JournalProxies journal;
    protected final NamespaceInfo namespace_info;

    public static class JournalProxies {
        protected final Configuration configuration;
        protected final Queue<InetSocketAddress> journal_servers;
        protected InetSocketAddress current;
        protected final QJournalProtocol protocol;

        public JournalProxies(Queue<InetSocketAddress> servers, Configuration configuration) throws IOException {
            this.configuration = configuration;
            this.journal_servers = servers;
            this.protocol = createJournal();
        }

        public InetSocketAddress address() {
            return current;
        }

        public QJournalProtocol protocol() {
            return protocol;
        }

        protected QJournalProtocol next() throws IOException {
            RPC.setProtocolEngine(configuration,
                    QJournalProtocolPB.class, ProtobufRpcEngine.class);

            // recycle
            InetSocketAddress address = journal_servers.poll();
            journal_servers.offer(address);

            LOGGER.info("connect to:" + address.getHostName());
            QJournalProtocolPB pbproxy = RPC.getProxy(
                    QJournalProtocolPB.class,
                    RPC.getProtocolVersion(QJournalProtocolPB.class),
                    address, configuration);

            current = address;
            return new QJournalProtocolTranslatorPB(pbproxy);
        }

        protected QJournalProtocol createJournal() throws IOException {
            QJournalProtocol connection = next();

            return (QJournalProtocol) Proxy.newProxyInstance(
                    Thread.currentThread().getContextClassLoader(),
                    new Class[]{QJournalProtocol.class},
                    new InvocationHandler() {
                        QJournalProtocol delegate = connection;

                        @Override
                        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                            try {
                                return method.invoke(delegate, args);
                            } catch (Throwable throwable) {
                                QJournalProtocol old_connection = null;
                                InetSocketAddress local = current;
                                synchronized (this) {
                                    if (local.equals(current)) {
                                        old_connection = delegate;
                                        delegate = next();
                                    }
                                }

                                // stop old one
                                if (old_connection != null) {
                                    RPC.stopProxy(old_connection);
                                }

                                // try again
                                return method.invoke(delegate, args);
                            }
                        }
                    }
            );
        }


    }

    public EditLogTailer(URI nameservice, URI journal) throws IOException {
        Configuration configuration = newConfiguration();

        //qjournal://10.202.77.200:8485;10.202.77.201:8485;10.202.77.202:8485/test-cluster-journal
        journal_id = journal.getPath().substring(1);
        Queue<InetSocketAddress> journal_servers = Arrays.stream(journal.getAuthority().split(";"))
                .map((server) -> {
                    return NetUtils.createSocketAddr(server, DFSConfigKeys.DFS_JOURNALNODE_RPC_PORT_DEFAULT);
                }).collect(Collectors.toCollection(ConcurrentLinkedQueue::new));

        // set hdfs
        generateHdfsHAConfiguration(nameservice)
                .forEach(configuration::set);

        URI uri = URI.create(configuration.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
        this.namenode = NameNodeProxies.createProxy(configuration, uri, NamenodeProtocol.class).getProxy();
        this.factory = URLConnectionFactory.newDefaultURLConnectionFactory(configuration);
        this.configuration = configuration;
        this.journal = new JournalProxies(journal_servers, configuration);
        this.namespace_info = namenode.versionRequest();
    }

    protected Configuration newConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(
                CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
                true);
        return configuration;
    }

    protected ImmutableMap<String, String> generateHdfsHAConfiguration(URI config) {
        // config example "test-cluster://10.202.77.200:8020,10.202.77.201:8020"
        ImmutableMap.Builder<String, String> generated = ImmutableMap.builder();

        // hdfs
        if (config.getHost() == null) {
            // default fs
            String nameservice = config.getScheme();
            generated.put(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + nameservice);

            // nameservice ha provider
            generated.put(DFSConfigKeys.DFS_NAMESERVICES, nameservice);
            generated.put(DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + nameservice, ConfiguredFailoverProxyProvider.class.getName());

            // set namenodes
            generated.put(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + nameservice, //
                    Arrays.stream(config.getAuthority().split(",")) //
                            .map((host) -> {
                                generated.put(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nameservice + "." + host, host);
                                return host;
                            }) //
                            .collect(Collectors.joining(",")) //
            );
        } else {
            // non ha
            generated.put(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://" + config.getHost());
        }

        // hdfs implementation
        generated.put("fs.hdfs.impl", DistributedFileSystem.class.getName());

        return generated.build();
    }


    protected Stream<FSEditLogOp> findAppropriateEdits() throws IOException {

        // save info
        QJournalProtocol protocol = journal.protocol();
        InetSocketAddress address = journal.address();

        // build request
        QJournalProtocolProtos.GetEditLogManifestResponseProto response = protocol.getEditLogManifest(journal_id, 0, true);
        RemoteEditLogManifest manifest = PBHelper.convert(response.getManifest());

        URI journal_http_or_https = URI.create(response.getFromURL());

        return manifest.getLogs().stream().parallel()
                .map((editlog) -> {
                    String path = GetJournalEditServlet.buildPath(
                            journal_id, editlog.getStartTxId(), namespace_info);
                    try {
                        URL edit_log_fetch = new URL(journal_http_or_https.getScheme(), address.getHostName(), journal_http_or_https.getPort(), path);

                        return EditLogFileInputStream.fromUrl(
                                factory,
                                edit_log_fetch,
                                editlog.getStartTxId(),
                                editlog.getEndTxId(),
                                false
                        );
                    } catch (MalformedURLException e) {
                        throw new RuntimeException("fail to create edit log fetch url for:" + editlog, e);
                    }
                })
                .flatMap((stream) -> {
                    return StreamSupport.stream(new Iterable<FSEditLogOp>() {
                        @Override
                        public Iterator<FSEditLogOp> iterator() {

                            return new Iterator<FSEditLogOp>() {
                                FSEditLogOp op = null;

                                @Override
                                public boolean hasNext() {
                                    try {
                                        op = stream.readOp();
                                    } catch (IOException e) {
                                        throw new UncheckedIOException("fail to read op for edit log:" + stream, e);
                                    }
                                    return op != null;
                                }

                                @Override
                                public FSEditLogOp next() {
                                    return op;
                                }
                            };
                        }
                    }.spliterator(), true);
                });
    }
}
