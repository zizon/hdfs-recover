package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.server.GetJournalEditServlet;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class LogSegment implements Iterable<FSEditLogOp> {

    public static final Log LOGGER = LogFactory.getLog(LogSegment.class);

    protected static final URLConnectionFactory URL_CONNECTION_FACTORY = URLConnectionFactory.newDefaultURLConnectionFactory(new Configuration());

    protected final URL fetch;

    protected final long from;
    protected final long to;

    public LogSegment(URI fetch_server, String log_name, NamespaceInfo namespace, long start, long end) {
        this.from = start;
        this.to = end;
        String path = GetJournalEditServlet.buildPath(log_name, start, namespace);
        try {
            this.fetch = new URL(fetch_server.getScheme(), fetch_server.getHost(), fetch_server.getPort(), path);
        } catch (MalformedURLException e) {
            throw new RuntimeException("fail to construct fetch url for:" + fetch_server);
        }
    }

    public long from() {
        return from;
    }

    public long to() {
        return to;
    }

    @Override
    public Iterator<FSEditLogOp> iterator() {
        EditLogInputStream stream = EditLogFileInputStream.fromUrl(
                URL_CONNECTION_FACTORY,
                fetch,
                from,
                to,
                true
        );

        return new Iterator<FSEditLogOp>() {
            FSEditLogOp op = null;
            AtomicLong counter = new AtomicLong(0);

            @Override
            public boolean hasNext() {
                op = nextOp(stream);
                counter.incrementAndGet();
                if (op == null) {
                    LOGGER.info("transation:" + counter.get());
                }
                return op != null;
            }

            @Override
            public FSEditLogOp next() {
                return op;
            }
        };
    }

    protected FSEditLogOp nextOp(EditLogInputStream stream) {
        try {
            return stream.readOp();
        } catch (IOException e) {
            throw new UncheckedIOException("fail to read stream:" + stream, e);
        }
    }

    @Override
    public String toString() {
        return "from:" + from + " to:" + to + " transations:" + (to - from);
    }
}
