package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LazyIterators {

    public static final Log LOGGER = LogFactory.getLog(LazyIterators.class);

    public static class MemorialIterator<T> implements Iterator<T> {

        protected final Iterator<T> delegate;
        protected T value;

        protected MemorialIterator(Iterator<T> iterator) {
            this.delegate = iterator;
            this.value = null;
        }

        public T value() {
            return value;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public T next() {
            value = delegate.next();
            return value;
        }
    }

    protected static class PrefetchIterator<T> implements Iterator<T> {

        protected Promise<Optional<T>> fetching;
        protected final Iterator<T> delegate;

        protected PrefetchIterator(Iterator<T> delegate) {
            this.delegate = delegate;
            this.fetching = prefetch();
        }

        protected Promise<Optional<T>> prefetch() {
            return Promise.light(() -> {
                if (delegate.hasNext()) {
                    return Optional.of(delegate.next());
                }

                // a true null indicate end of iterator
                return Optional.empty();
            });
        }

        @Override
        public boolean hasNext() {
            return this.fetching.join().isPresent();
        }

        @Override
        public T next() {
            T value = this.fetching.join().orElseThrow(() -> new NoSuchElementException("end of stream"));
            fetching = prefetch();
            return value;
        }
    }

    public static <T> Iterator<T> concat(Stream<Promise.PromiseSupplier<Iterator<T>>> iterator_stream) {
        return new Iterator<T>() {
            Iterator<Promise.PromiseSupplier<Iterator<T>>> iterator_generator = iterator_stream.iterator();
            Iterator<T> current = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                if (current.hasNext()) {
                    return true;
                }

                if (iterator_generator.hasNext()) {
                    current = iterator_generator.next().get();
                    return hasNext();
                }

                return false;
            }

            @Override
            public T next() {
                return current.next();
            }
        };
    }

    public static <T> Iterator<T> merge(Comparator<T> comparator, Collection<Iterator<T>> iterators) {
        // poll value
        Collection<MemorialIterator<T>> memorials = iterators.parallelStream()
                .filter(Iterator::hasNext)
                .map(MemorialIterator::new)
                .peek(MemorialIterator::next)
                .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));

        return generate(() -> {
            // compare base on value
            Optional<T> min = memorials.parallelStream()
                    .map(MemorialIterator::value)
                    .min(comparator);

            // advacen selected iterator
            min.ifPresent((value) -> {
                memorials.parallelStream()
                        .forEach((iterator) -> {
                            if (comparator.compare(value, iterator.value()) == 0) {
                                if (iterator.hasNext()) {
                                    iterator.next();
                                    return;
                                }

                                // no more,remove this
                                memorials.remove(iterator);
                            }

                            // not match,keep value,and do nothing
                            return;
                        });
            });

            // then advance
            return min;
        });
    }

    public static <T> Iterator<T> generate(Promise.PromiseSupplier<Optional<T>> generator) {
        return new Iterator<T>() {
            Optional<T> resolved_value = resolveNext();

            Optional<T> resolveNext() {
                return generator.get();
            }

            @Override
            public boolean hasNext() {
                return resolved_value.isPresent();
            }

            @Override
            public T next() {
                T value = resolved_value.orElseThrow(() -> new NoSuchElementException("end of iterator"));
                resolved_value = resolveNext();
                return value;
            }
        };
    }

    public static <T> Iterator<T> prefetch(Iterator<T> iterator) {
        if (iterator instanceof PrefetchIterator) {
            return iterator;
        }

        return new PrefetchIterator<>(iterator);
    }

    public static <T> MemorialIterator<T> memorial(Iterator<T> iterator) {
        if (iterator instanceof MemorialIterator) {
            return (MemorialIterator<T>) iterator;
        }

        return new MemorialIterator<>(iterator);
    }

    public static <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(((Iterable<T>) () -> iterator).spliterator(), false);
    }

    public static <T> Stream<T> stream(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
