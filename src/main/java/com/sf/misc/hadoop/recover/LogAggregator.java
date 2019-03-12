package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class LogAggregator implements Iterable<FSEditLogOp> {

    public static final Log LOGGER = LogFactory.getLog(LogAggregator.class);

    protected final URI nameservice;
    protected final String runas;

    public LogAggregator(URI nameservice, String runas) {
        this.nameservice = nameservice;
        this.runas = runas;
    }

    public Promise<List<Promise<LogServer>>> logServers() {
        Promise<NamespaceInfo> namespace = findNameSpace(nameservice);
        Promise<URI> jouranl_server = findJournalServer(nameservice);

        return jouranl_server.transform((journal) -> {
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
                                info
                        );
                    })
                    .map((genetator) -> {
                        return namespace.transform((info) -> genetator.apply(info));
                    })
                    .collect(Collectors.toList());
        });
    }

    protected Promise<NamespaceInfo> findNameSpace(URI nameservice) {
        Configuration configuration = generateHdfsHAConfiguration(nameservice);

        // create namnode protocol
        Promise<NamenodeProtocol> namenode = Promise.light(() -> {
            URI uri = URI.create(configuration.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
            return UserGroupInformation.createRemoteUser(runas).doAs((PrivilegedExceptionAction<NamenodeProtocol>) () -> {
                return NameNodeProxies.createProxy(configuration, uri, NamenodeProtocol.class).getProxy();
            });
        });

        // maybe set namespace info
        return namenode.transform(NamenodeProtocol::versionRequest)
                .sidekick(() -> RPC.stopProxy(namenode.join()));
    }

    protected Promise<URI> findJournalServer(URI nameservice) {
        Promise<Configuration>[] racing = Arrays.stream(nameservice.getAuthority().split(","))
                .map((host_with_port) -> {
                    return URI.create("any://" + host_with_port).getHost();
                })
                .map((host) -> {
                    return URI.create("http://" + host + ":50070/conf");
                })
                .map((uri) -> {
                    // ask namenode
                    return Promise.light(() -> {
                        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
                        try {
                            Configuration configuration = new Configuration();
                            configuration.addResource(connection.getInputStream());

                            // trigger load
                            configuration.size();

                            return configuration;
                        } finally {
                            Optional.ofNullable(connection).ifPresent(HttpURLConnection::disconnect);
                        }
                    });
                })
                .<Promise<Configuration>>toArray(Promise[]::new);

        // then extract jounral node
        return Promise.either(racing).transform((configuration) -> {
            return URI.create(configuration.get(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY));
        });
    }

    protected Configuration generateHdfsHAConfiguration(URI config) {
        // config example "test-cluster://10.202.77.200:8020,10.202.77.201:8020"
        Configuration generated = new Configuration();

        // hdfs
        if (config.getHost() == null) {
            // default fs
            String nameservice = config.getScheme();
            generated.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + nameservice);

            // nameservice ha provider
            generated.set(DFSConfigKeys.DFS_NAMESERVICES, nameservice);
            generated.set(DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + nameservice, ConfiguredFailoverProxyProvider.class.getName());

            // set namenodes
            generated.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + nameservice, //
                    Arrays.stream(config.getAuthority().split(",")) //
                            .map((host) -> {
                                generated.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nameservice + "." + host, host);
                                return host;
                            }) //
                            .collect(Collectors.joining(",")) //
            );
        } else {
            // non ha
            generated.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://" + config.getHost());
        }

        // hdfs implementation
        generated.set("fs.hdfs.impl", DistributedFileSystem.class.getName());

        return generated;
    }


    public Iterator<FSEditLogOp> skipUntil(long txid) {
        Promise<NamespaceInfo> namespace = findNameSpace(nameservice);
        Promise<URI> jouranl_server = findJournalServer(nameservice);

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
                                info
                        );
                    })
                    .map((genetator) -> {
                        return namespace.transform((info) -> genetator.apply(info).skipUntil(txid));
                    })
                    .collect(Promise.listCollector());
        }).transform((iterators) -> {
            return LazyIterators.merge(
                    Comparator.comparing(FSEditLogOp::getTransactionId),
                    iterators
            );
        }).join();
    }

    @Override
    public Iterator<FSEditLogOp> iterator() {
        Promise<NamespaceInfo> namespace = findNameSpace(nameservice);
        Promise<URI> jouranl_server = findJournalServer(nameservice);

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
                                info
                        );
                    })
                    .map((genetator) -> {
                        return namespace.transform((info) -> genetator.apply(info).iterator());
                    })
                    .collect(Promise.listCollector());
        }).transform((iterators) -> {
            return LazyIterators.merge(
                    Comparator.comparing(FSEditLogOp::getTransactionId),
                    iterators
            );
        }).join();
    }
}
