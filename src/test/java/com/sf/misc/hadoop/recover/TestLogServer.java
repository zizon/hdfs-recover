package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

public class TestLogServer {

    public static final Log LOGGER = LogFactory.getLog(TestLogServer.class);

    @Test
    public void test() {
        //int nsID, String clusterID, String bpID,
        //      long cT, String buildVersion, String softwareVersion
        LogServer server = new LogServer(
                new InetSocketAddress("10.202.77.201", 8485),
                "test-cluster-journal",
                new NamespaceInfo(1095198515, "CID-00865301-72d3-4468-8bf2-7533f99ff254", "BP-1324004911-10.202.77.200-1497948758008", 0)
        );

        Promise<?> segment_test = server.segments().transform((segment) -> {
            segment.stream().parallel().forEach((value) -> {
                Assert.assertEquals("segment fail:" + value,
                        value.to() - value.from() + 1,
                        LazyIterators.stream(value).parallel().count()
                );
            });
            return null;
        });

        Promise<?> server_test = Promise.light(() -> {

            NavigableSet<Long> txids = new ConcurrentSkipListSet<>(Long::compareTo);
            Queue<Long> all = LazyIterators.stream(server).parallel()
                    .map(FSEditLogOp::getTransactionId)
                    .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));

            txids.addAll(all);
            Assert.assertEquals("server iterator fail", txids.last() - txids.first() + 1, txids.size());
            Assert.assertEquals("size match:", all.size(), txids.size());
        });

        Promise.all(segment_test, server_test).join();
    }
}
