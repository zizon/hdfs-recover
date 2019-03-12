package com.sf.misc.hadoop.recover;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LazyIterators {

    protected static class Peekable<T> {

        protected final Iterator<T> iterator;
        protected boolean emtpy;
        protected T head;

        protected Peekable(Iterator<T> iterator) {
            this.iterator = iterator;
            this.emtpy = false;
            this.advance();
        }

        public boolean isEmtpy() {
            return emtpy;
        }

        public T head() {
            return head;
        }

        public void advance() {
            if (!iterator.hasNext()) {
                this.emtpy = true;
                return;
            }

            head = iterator.next();
            return;
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
        return new Iterator<T>() {
            Collection<Peekable<T>> peekables = decorate(iterators);
            Optional<T> resolved_value = resolveNext();

            Optional<T> resolveNext() {
                peekables.removeIf(Peekable::isEmtpy);

                //2. peek head
                return peekables.parallelStream()
                        .map(Peekable::head)
                        .min(comparator)
                        .map((value) -> {
                            // then advance peekable if head match value
                            peekables = peekables.parallelStream()
                                    .map((peekable) -> {
                                        if (comparator.compare(value, peekable.head()) == 0) {
                                            peekable.advance();
                                        }
                                        return peekable;
                                    })
                                    .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
                            return value;
                        });
            }

            @Override
            public boolean hasNext() {
                return resolved_value.isPresent();
            }

            @Override
            public T next() {
                T value = resolved_value.get();
                resolved_value = resolveNext();
                return value;
            }
        };
    }

    public static <T> Iterator<T> generate(Promise.PromiseSupplier<Optional<T>> generator) {
        return new Iterator<T>() {
            Optional<T> resolved_value = resolveNext();

            Optional<T> resolveNext() {
                return resolved_value = generator.get();
            }

            @Override
            public boolean hasNext() {
                return resolved_value.isPresent();
            }

            @Override
            public T next() {
                T value = resolved_value.get();
                resolved_value = resolveNext();
                return value;
            }
        };
    }

    public static <T> Iterator<T> filter(Iterator<T> iterator, Predicate<T> predicate) {
        return new Iterator<T>() {

            Optional<T> resolved = resolveNext();

            Optional<T> resolveNext() {
                while (iterator.hasNext()) {
                    T value = iterator.next();
                    if (predicate.test(value)) {
                        return Optional.of(value);
                    }
                }

                return Optional.empty();
            }

            @Override
            public boolean hasNext() {
                return resolved.isPresent();
            }

            @Override
            public T next() {
                T value = resolved.get();
                resolved = resolveNext();
                return value;
            }
        };
    }

    protected static <T> Collection<Peekable<T>> decorate(Collection<Iterator<T>> iterators) {
        return iterators.parallelStream().map(Peekable::new).collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
    }
}
