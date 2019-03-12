package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class TestEditLogTailer {

    public static final Log LOGGER = LogFactory.getLog(TestEditLogTailer.class);

    @Test
    public void testRenameOldReflection() throws Throwable {
        URI nameservice = URI.create("test-cluster://10.202.77.200:8020,10.202.77.201:8020");
        File storage = new File("__storage__");
        new EditLogTailer(
                storage,
                nameservice,
                "hdfs",
                (op) -> {
                    // reject not rename op
                    if (op.opCode.compareTo(FSEditLogOpCodes.OP_RENAME_OLD) != 0) {
                        return false;
                    }

                    // skip if not in trash
                    if (!RenameOldOpSerializer.target(op).contains(".Trash")) {
                        //return false;
                    }
                    return true;
                },
                RenameOldOpSerializer::lineSerialize
        ).start(
                TimeUnit.MINUTES.toMillis(1),
                TimeUnit.HOURS.toMillis(1)
        ).logException().join();
    }
}
