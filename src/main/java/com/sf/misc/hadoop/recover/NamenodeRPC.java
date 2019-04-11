package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.HttpURLConnection;
import java.net.URI;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class NamenodeRPC implements AutoCloseable {

    public static final Log LOGGER = LogFactory.getLog(NamenodeRPC.class);

    protected final ConcurrentMap<Class<?>, Promise<Object>> protocols = new ConcurrentHashMap<>();
    protected final Promise.PromiseFunction<Class<?>, Promise<Object>> protocol_provider;
    protected final Promise<Configuration> configuration;
    protected final Promise<FileSystem> fs;
    protected final Promise<DFSClient> client;

    public NamenodeRPC(URI nameservice, String runas) {
        protocol_provider = (protocol) -> {
            return Promise.light(() -> {
                Configuration configuration = generateHdfsHAConfiguration(nameservice);
                URI uri = URI.create(configuration.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
                return doas(() -> NameNodeProxies.createProxy(configuration, uri, protocol).getProxy(), runas);
            });
        };

        this.configuration = fetchLiveConfiguration(nameservice);

        this.fs = Promise.lazy(() -> {
            return Promise.success(
                    doas(
                            () -> FileSystem.get(generateHdfsHAConfiguration(nameservice)),
                            runas
                    )
            );
        });

        this.client = Promise.lazy(() -> {
            return Promise.success(
                    doas(
                            () -> {
                                Configuration configuration = generateHdfsHAConfiguration(nameservice);

                                return new DFSClient(URI.create(configuration.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY)), configuration);
                            },
                            runas
                    )
            );
        });
    }

    protected <T> T doas(Promise.PromiseSupplier<T> supplier, String runas) {
        return UserGroupInformation.createRemoteUser(runas).doAs((PrivilegedAction<T>) supplier::get);
    }

    public <T> Promise<T> protocol(Class<T> type) {
        return (Promise<T>) protocols.compute(
                type,
                (key, value) -> Optional.ofNullable(value)
                        .orElseGet(() -> protocol_provider.apply(type))
        );
    }

    public Promise<Configuration> configuration() {
        return configuration;
    }

    public Promise<FileSystem> fs() {
        return this.fs;
    }

    public Promise<DFSClient> client() {
        return client;
    }

    @Override
    public void close() {
        this.protocols.entrySet().parallelStream()
                .map((entry) -> {
                    return entry.getValue().transform((protocol) -> {
                        RPC.stopProxy(protocol);
                        return entry.getKey();
                    });
                })
                .collect(Promise.collector())
                .join();
    }

    protected Promise<Configuration> fetchLiveConfiguration(URI nameservice) {
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
        return Promise.either(racing);
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
                            .peek((host) -> {
                                generated.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nameservice + "." + host, host);
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


}
