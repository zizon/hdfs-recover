package com.sf.misc.hadoop.recover;

import com.google.common.collect.Iterators;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

public class LazyIterators {

    public static <T> Iterator<T> concat(Stream<Promise.PromiseSupplier<Iterator<T>>> iterator_stream) {
        return new Iterator<T>() {
            Iterator<Promise.PromiseSupplier<Iterator<T>>> lazy = iterator_stream.sequential().iterator();
            Iterator<T> current;
            T value;

            @Override
            public boolean hasNext() {
                if (current == null) {
                    if (lazy.hasNext()) {
                        current = lazy.next().get();
                    } else {
                        // no iterators
                        return false;
                    }
                }

                if (current.hasNext()) {
                    value = current.next();
                    return true;
                }

                // current is consuemd
                current = null;
                return hasNext();
            }

            @Override
            public T next() {
                T result = value;
                value = null;
                return result;
            }
        };
    }

    public static <T> Iterator<T> concat(Promise.PromiseSupplier<Iterator<T>>... iterators) {
        return concat(Arrays.stream(iterators));
    }
}
