package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.server.GetJournalEditServlet;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

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

    public static Iterator<FSEditLogOp> merge(Collection<LogSegment> segments) {
        return new Iterator<FSEditLogOp>() {
            Iterator<FSEditLogOp> delegate = LazyIterators.concat(
                    segments.stream()
                            .sorted(Comparator.comparing(LogSegment::from))
                            .map((segment) -> {
                                return (Promise.PromiseSupplier<Iterator<FSEditLogOp>>) () -> segment.iterator();
                            })
            );

            FSEditLogOp op;
            long last_txid = HdfsConstants.INVALID_TXID;

            @Override
            public boolean hasNext() {
                if (!delegate.hasNext()) {
                    return false;
                }

                // skip to appropriate txid
                FSEditLogOp next = delegate.next();
                while (!next.hasTransactionId() || next.getTransactionId() <= last_txid) {
                    if (delegate.hasNext()) {
                        next = delegate.next();
                        continue;
                    }

                    return false;
                }

                op = next;
                return true;
            }

            @Override
            public FSEditLogOp next() {
                FSEditLogOp result = op;
                op = null;
                return result;
            }
        };
    }

    @Override
    public Iterator<FSEditLogOp> iterator() {
        return new Iterator<FSEditLogOp>() {
            Promise<EditLogInputStream> stream = Promise.light(
                    () -> EditLogFileInputStream.fromUrl(
                            URL_CONNECTION_FACTORY,
                            fetch,
                            from,
                            to,
                            true
                    )
            );
            FSEditLogOp op;

            @Override
            public boolean hasNext() {
                try {
                    op = FSEditLogOpSniper.copy(stream.join().readOp());

                    if (op != null) {
                        return true;
                    }
                    return false;
                } catch (IOException e) {
                    throw new UncheckedIOException("fail to tail log:" + this, e);
                }
            }

            @Override
            public FSEditLogOp next() {
                return op;
            }
        };
    }

    @Override
    public String toString() {
        return "from:" + from + " to:" + to + " transations:" + (to - from);
    }
}
