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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
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
        Iterator<FSEditLogOp> concated = LazyIterators.concat(
                segments.stream().parallel()
                        .sorted(Comparator.comparing(LogSegment::from))
                        .map((segment) -> segment::iterator)
        );

        LazyIterators.MemorialIterator<FSEditLogOp> maybe_duplicated = LazyIterators.memorial(
                LazyIterators.stream(concated).parallel()
                        .filter(FSEditLogOp::hasTransactionId)
                        .iterator()
        );

        return LazyIterators.stream(
                new Iterator<FSEditLogOp>() {
                    @Override
                    public boolean hasNext() {
                        return maybe_duplicated.hasNext();
                    }

                    @Override
                    public FSEditLogOp next() {
                        FSEditLogOp current = maybe_duplicated.value();
                        if (current == null) {
                            return maybe_duplicated.next();
                        }

                        do {
                            FSEditLogOp next = maybe_duplicated.next();
                            if (current.getTransactionId() > next.getTransactionId()) {
                                continue;
                            }

                            return next;
                        } while (maybe_duplicated.hasNext());

                        return null;
                    }
                }
        ).parallel().filter(Objects::nonNull).iterator();
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

    public Iterator<FSEditLogOp> skipUntil(long txid) {
        if (txid > this.to()) {
            return Collections.emptyIterator();
        }

        EditLogInputStream stream = stream();
        return LazyIterators.stream(
                LazyIterators.generate(() ->
                        Optional.ofNullable(
                                FSEditLogOpSniper.copy(stream.readOp())
                        )
                )
        ).filter(FSEditLogOp::hasTransactionId)
                .filter((op) -> op.getTransactionId() > txid).iterator();
    }

    @Override
    public Iterator<FSEditLogOp> iterator() {
        return skipUntil(HdfsConstants.INVALID_TXID);
    }

    @Override
    public String toString() {
        return "from:" + from + " to:" + to + " transactions:" + (to - from);
    }
}
