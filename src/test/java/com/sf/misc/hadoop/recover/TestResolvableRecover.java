package com.sf.misc.hadoop.recover;


import com.jcraft.jsch.SftpException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TestResolvableRecover {

    public static final Log LOGGER = LogFactory.getLog(TestResolvableRecover.class);

    @Test
    public void test() {
        URI nameservice = URI.create("test-cluster://10.202.77.200:8020,10.202.77.201:8020");
        NamenodeRPC namendoe = new NamenodeRPC(nameservice, "hdfs");
        File storage = new File("__storage__");

        LogAggregator aggregator = new LogAggregator(namendoe, true);
        EditLogArchive archive = new EditLogArchive(storage);

        Promise<?> tailing = new EditLogTailer(
                archive,
                aggregator,
                (op) -> op.opCode.compareTo(FSEditLogOpCodes.OP_RENAME_OLD) == 0,
                RenameOldOpSerializer::lineSerialize,
                (stat) -> {
                    //LOGGER.info("stat:" + stat);
                }
        ).start(1, TimeUnit.DAYS.toMillis(365));

        Promise.light(() -> {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
            long start = format.parse("20190322000000").getTime();
            long end = format.parse("20190901000000").getTime();

            return new ResolvableRecover(
                    aggregator,
                    archive,
                    new ResolvableRecover.RecoverInterceptor() {
                        @Override
                        public boolean dryrun() {
                            return false;
                        }

                        @Override
                        public Path transformTarget(Path target) {
                            Path base = new Path("/tmp/recover_test");
                            String raw = target.toUri().getPath();
                            Path new_target = new Path(base, raw.substring(1));
                            //LOGGER.info(target + " -> " + new_target);
                            return new_target;
                        }

                        @Override
                        public void onRenameFail(WriteEditLogOP op, Throwable reason) {
                            LOGGER.error("fail op:" + op, reason);
                        }

                        @Override
                        public void onRenameOk(WriteEditLogOP op) {
                            //LOGGER.info("ok op:" +op);
                        }
                    },
                    namendoe.fs().join(),
                    namendoe.client().join()
            ).replay(start, end);
        }).transformAsync((through) -> through).join();

        LOGGER.info("done");
        tailing.cancel(true);
        tailing.maybe();
        LOGGER.info("all finished");
    }
}
