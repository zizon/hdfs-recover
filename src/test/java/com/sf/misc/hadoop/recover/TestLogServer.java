package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

        LOGGER.info("count:" + StreamSupport.stream(server.spliterator(), true).count());


        StreamSupport.stream(server.spliterator(), true)
                .forEach((op) -> {
                    //LOGGER.info(op.getTransactionId());
                });
    }
}
