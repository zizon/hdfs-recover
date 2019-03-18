package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.concurrent.TimeUnit;

public class TestEditLogTailer {

    public static final Log LOGGER = LogFactory.getLog(TestEditLogTailer.class);

    @Test
    public void test() throws Throwable {
        URI nameservice = URI.create("test-cluster://10.202.77.200:8020,10.202.77.201:8020");
        File storage = new File("__storage__");

        new EditLogTailer(
                storage,
                new NamenodeRPC(nameservice, "hdfs"),
                (op) -> {
                    // reject not rename op
                    if (op.opCode.compareTo(FSEditLogOpCodes.OP_RENAME_OLD) != 0) {
                        return false;
                    }

                    // skip if not in trash
                    if (!RenameOldOpSerializer.target(op).contains(".Trash")) {
                        return false;
                    }
                    return true;
                },
                RenameOldOpSerializer::lineSerialize,
                false,
                (stat) -> {
                    LOGGER.info("stat:" + stat);
                }
        ).start(
                TimeUnit.MINUTES.toMillis(1),
                TimeUnit.DAYS.toMillis(365)
        ).logException().join();
    }
}
