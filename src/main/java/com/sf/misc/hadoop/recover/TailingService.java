package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TailingService {

    public static final Log LOGGER = LogFactory.getLog(TailingService.class);

    public static void main(String[] args) {
        // load config
        Properties properties = new Properties();
        URL config = Thread.currentThread().getContextClassLoader().getResource("config.property");
        if (config == null) {
            throw new UncheckedIOException(new FileNotFoundException("no config.property found"));
        }

        try (InputStream input = config.openStream()) {
            properties.load(input);
        } catch (IOException e) {
            throw new UncheckedIOException("fail to open config:" + config, e);
        }

        // print
        properties.forEach((key, value) -> {
            LOGGER.info("using config:" + key + " = " + value);
        });

        new EditLogTailer(
                new File(properties.getOrDefault("storage", "__storage__").toString()),
                new NamenodeRPC(
                        URI.create(properties.get("nameservice").toString()),
                        properties.getProperty("runas", "hdfs")
                ),
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
        ).start(Long.valueOf(properties.getOrDefault("poll_period", "" + TimeUnit.MINUTES.toMillis(1)).toString()),
                Long.valueOf(properties.getOrDefault("expiration_for_log", "" + TimeUnit.DAYS.toMillis(365)).toString())
        ).logException().join();
    }
}
