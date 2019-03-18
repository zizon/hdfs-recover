package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

public class TestLogAggregator {

    public static final Log LOGGER = LogFactory.getLog(TestLogAggregator.class);


    @Test
    public void test() {
        URI nameservice = URI.create("test-cluster://10.202.77.200:8020,10.202.77.201:8020");
        NamenodeRPC namenode = new NamenodeRPC(nameservice, "hdfs");
        LogAggregator aggregator = new LogAggregator(namenode,false);

        Queue<Long> all = LazyIterators.stream(aggregator).parallel()
                .map(FSEditLogOp::getTransactionId)
                .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));

        NavigableSet<Long> txids = new ConcurrentSkipListSet<>(all);

        LOGGER.info(txids.size());
        Assert.assertEquals("aggreatro fail", txids.last() - txids.first() + 1, txids.size());
        Assert.assertEquals("size match:", all.size(), txids.size());

        namenode.close();
    }
}
