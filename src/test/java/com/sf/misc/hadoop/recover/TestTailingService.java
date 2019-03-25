package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

public class TestTailingService {

    public static final Log LOGGER = LogFactory.getLog(TestTailingService.class);

    @Test
    public void test() throws Throwable {
        TailingService.startHTTPServer(10088);
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

        HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:10088").openConnection();
        LOGGER.info("response:" + connection.getResponseCode());
        connection.getHeaderFields()
                .forEach((key, values) -> {
                    LOGGER.info("key:" + key + " value:" + values.stream().collect(Collectors.joining(",")));
                });
        InputStream stream = connection.getInputStream();
        byte[] buffer = new byte[1024];
        int readed = 0;
        while ((readed = stream.read(buffer)) != -1) {
            System.out.print(new String(buffer, 0, readed));
        }

        LOGGER.info("done?");
    }
}
