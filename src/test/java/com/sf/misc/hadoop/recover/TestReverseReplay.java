package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.net.URI;

public class TestReverseReplay {

    public static final Log LOGGER = LogFactory.getLog(TestReverseReplay.class);

    @Test
    public void test() {
        URI nameservice = URI.create("test-cluster://10.202.77.200:8020,10.202.77.201:8020");
        NamenodeRPC namendoe = new NamenodeRPC(nameservice, "hdfs");
        File storage = new File("__storage__");

        // walk around windows shell problem
        /*
        Promise.light(() -> FileSystem.getLocal(new Configuration())).transform((fs) -> {
            fs.close();
            return null;
        }).join();
        */

        ResolvableRecover.RecoverInterceptor interceptor = new ResolvableRecover.RecoverInterceptor() {
            @Override
            public Path transformTarget(Path target) {
                Path base = new Path("/tmp/recover_test");
                String raw = target.toUri().getPath();
                Path new_target = new Path(base, raw.substring(1));
                //LOGGER.info(target + " -> " + new_target);
                return new_target;
            }

            @Override
            public boolean dryrun() {
                return false;
            }
        };

        namendoe.fs().transform((fs) -> {
            return new ReverseReplay(
                    new EditLogArchive(storage),
                    fs,
                    interceptor
            );
        }).transformAsync((replay) -> replay.forRange(0, Long.MAX_VALUE)).join();
    }
}
