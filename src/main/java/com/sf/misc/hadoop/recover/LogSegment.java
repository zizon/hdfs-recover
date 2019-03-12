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

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;

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

    public static Iterator<FSEditLogOp> merge(Collection<LogSegment> segments) {
        return new Iterator<FSEditLogOp>() {
            Iterator<FSEditLogOp> delegate = LazyIterators.filter(
                    LazyIterators.concat(
                            segments.stream().parallel()
                                    .sorted(Comparator.comparing(LogSegment::from))
                                    .map((segment) -> segment::iterator)
                    ),
                    FSEditLogOp::hasTransactionId
            );

            long last_txid = HdfsConstants.INVALID_TXID;
            Optional<FSEditLogOp> resolved = resolveNext();

            Optional<FSEditLogOp> resolveNext() {
                while (delegate.hasNext()) {
                    FSEditLogOp next = delegate.next();
                    if (next.getTransactionId() > last_txid) {
                        last_txid = next.getTransactionId();
                        return Optional.of(next);
                    }
                }

                return Optional.empty();
            }


            @Override
            public boolean hasNext() {
                return resolved.isPresent();
            }

            @Override
            public FSEditLogOp next() {
                FSEditLogOp value = resolved.get();
                resolved = resolveNext();
                return value;
            }
        };
    }

    public long from() {
        return from;
    }

    public long to() {
        return to;
    }

    public EditLogInputStream stream() {
        return EditLogFileInputStream.fromUrl(
                URL_CONNECTION_FACTORY,
                fetch,
                from,
                to,
                true
        );
    }

    @Override
    public Iterator<FSEditLogOp> iterator() {
        EditLogInputStream stream = stream();
        return LazyIterators.generate(() -> {
            return Optional.ofNullable(FSEditLogOpSniper.copy(stream.readOp()));
        });
    }

    @Override
    public String toString() {
        return "from:" + from + " to:" + to + " transations:" + (to - from);
    }
}
