package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class RenameOldOpSerializer {

    public static final Log LOGGER = LogFactory.getLog(RenameOldOpSerializer.class);

    protected static final ConcurrentMap<String, MethodHandle> FIELD_ACCESSOR;

    public static class Rename {
        protected final long txid;
        protected final String source;
        protected final String target;
        protected final long timestamp;

        public Rename(long txid, String source, String target, long timestamp) {
            this.txid = txid;
            this.source = source;
            this.target = target;
            this.timestamp = timestamp;
        }

        public long txid() {
            return txid;
        }

        public String source() {
            return source;
        }

        public String target() {
            return target;
        }

        public long timestamp() {
            return timestamp;
        }
    }

    static {
        Class<?> rename_old = null;
        try {
            rename_old = Class.forName("org.apache.hadoop.hdfs.server.namenode.FSEditLogOp$RenameOldOp");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("no class found", e);
        }

        FIELD_ACCESSOR = Arrays.stream(rename_old.getDeclaredFields())
                .collect(Collectors.toConcurrentMap(
                        Field::getName
                        , (field) -> {
                            field.setAccessible(true);
                            try {
                                LOGGER.info("RenameOP field:" + field);
                                return MethodHandles.publicLookup().unreflectGetter(field);
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException("can not find getter for field:" + field, e);
                            }
                        }
                        )
                );
    }

    public static byte[] lineSerialize(FSEditLogOp op) {
        String source = source(op);
        String target = target(op);
        long timestampe = timestamp(op);
        String date = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(timestampe));

        return ("rename: "
                + ("txid: " + op.getTransactionId() + " ")
                + ("src: " + encode(source) + " ")
                + ("dst: " + encode(target) + " ")
                + ("date: " + date + " ")
                + ("timestamp: " + timestampe + "\n")
        ).getBytes();
    }

    public static Rename deserialize(String line) {
        List<String> parts = Arrays.stream(line.split(" ")).map((part) -> part.replace("\n", ""))
                .collect(Collectors.toList());

        String txid = parts.get(2);
        String source = decode(parts.get(4));
        String target = decode(parts.get(6));
        String timestamp = parts.get(10);

        return new Rename(Long.valueOf(txid), source, target, Long.valueOf(timestamp));
    }

    public static String source(FSEditLogOp op) {
        return invoke(op, "src").toString();
    }

    public static String target(FSEditLogOp op) {
        return invoke(op, "dst").toString();
    }

    public static long timestamp(FSEditLogOp op) {
        return (long) invoke(op, "timestamp");
    }

    public static long txid(FSEditLogOp op) {
        return op.getTransactionId();
    }

    protected static Object invoke(FSEditLogOp op, String key) {
        try {
            return FIELD_ACCESSOR.get(key).bindTo(op).invoke();
        } catch (Throwable throwable) {
            throw new RuntimeException("fail to get key:" + key + " of:" + op, throwable);
        }
    }

    protected static String encode(String input) {
        try {
            return URLEncoder.encode(input, "utf8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("unable to escape input:" + input + " using utf8", e);
        }
    }

    protected static String decode(String input) {
        try {
            return URLDecoder.decode(input, "utf8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("unable to unescape input:" + input + " using utf8", e);
        }
    }
}
