package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.security.PrivilegedAction;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TestLogAggregator {

    public static final Log LOGGER = LogFactory.getLog(TestLogAggregator.class);


    @Test
    public void test() {
        URI nameservice = URI.create("test-cluster://10.202.77.200:8020,10.202.77.201:8020");

        LogAggregator aggregator = new LogAggregator(nameservice, "hdfs");

        NavigableSet<Long> txids = StreamSupport.stream(aggregator.spliterator(), true).parallel()
                .map(FSEditLogOp::getTransactionId)
                .collect(Collectors.toCollection(ConcurrentSkipListSet::new));

        Assert.assertEquals("aggreatro fail", txids.last() - txids.first() + 1, txids.size());
    }
}
