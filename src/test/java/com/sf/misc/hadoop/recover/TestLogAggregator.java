package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.net.URI;
import java.security.PrivilegedAction;
import java.util.stream.StreamSupport;

public class TestLogAggregator {

    public static final Log LOGGER = LogFactory.getLog(TestLogAggregator.class);


    @Test
    public void doas() {
        UserGroupInformation.createRemoteUser("hdfs").doAs((PrivilegedAction<Object>) () -> {
            test();
            return null;
        });
    }

    public void test() {
        URI nameservice = URI.create("test-cluster://10.202.77.200:8020,10.202.77.201:8020");

        LogAggregator aggregator = new LogAggregator(nameservice, "hdfs");
        
        aggregator.logServers().transform((promises) -> {
            LOGGER.info(promises.size());
            promises.forEach((promise) -> {
                promise.transform((server) -> {
                    LOGGER.info("for server:" + server + " total:" + StreamSupport.stream(server.spliterator(), true).count());

                    return null;
                }).join();
            });

            return null;
        }).join();
    }
}
