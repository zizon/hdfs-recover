package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class TestEditLogTailer {

    public static final Log LOGGER = LogFactory.getLog(TestEditLogTailer.class);

    @Test
    public void test() {
        URI nameservice = URI.create("test-cluster://10.202.77.200:8020,10.202.77.201:8020");
        URI journal = URI.create("qjournal://10.202.77.202:8485;10.202.77.202:8485;10.202.77.202:8485/test-cluster-journal");
        doas("hdfs", () -> {
            try {
                Stream<FSEditLogOp> stream = new EditLogTailer(nameservice, journal).findAppropriateEdits();
                LOGGER.info(stream.count());
                /*
                stream.forEach((op) -> {
                    LOGGER.info(op);
                });
                */
            } catch (Throwable e) {
                LOGGER.error("fail", e);
                Assert.fail();
            }
        });
    }

    protected void doas(String user, Runnable runnable) {
        UserGroupInformation.createRemoteUser(user).doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                runnable.run();
                return null;
            }
        });
    }

}
