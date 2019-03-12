package com.sf.misc.hadoop.recover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Promise<T> extends CompletableFuture<T> {

    public static final Log LOGGER = LogFactory.getLog(Promise.class);

    public interface PromiseCallable<T> extends Callable<T> {

        T exceptionalCall() throws Throwable;

        default T call() {
            try {
                return this.exceptionalCall();
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected call exception", throwable);
            }
        }
    }

    public interface PromiseRunnable extends Runnable {
        void exceptionalRun() throws Throwable;

        default void run() {
            try {
                this.exceptionalRun();
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected run exception", throwable);
            }
        }
    }

    public interface PromiseFunction<T, R> extends Function<T, R> {

        R internalApply(T t) throws Throwable;

        default R apply(T t) {
            try {
                return this.internalApply(t);
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected function exception", throwable);
            }
        }
    }

    public interface PromiseConsumer<T> extends Consumer<T> {
        void internalAccept(T t) throws Throwable;

        default void accept(T t) {
            try {
                this.internalAccept(t);
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected consume exception", throwable);
            }
        }
    }

    public interface PromiseBiConsumer<T, U> extends BiConsumer<T, U> {
        void internalAccept(T first, U second) throws Throwable;

        default void accept(T t, U u) {
            try {
                this.internalAccept(t, u);
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected biconsume exception", throwable);
            }
        }
    }

    public interface PromiseBiFunction<T, U, R> extends BiFunction<T, U, R> {
        R internalApply(T first, U second) throws Throwable;

        default R apply(T t, U u) {
            try {
                return this.internalApply(t, u);
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected biconsume exception", throwable);
            }
        }
    }

    public interface PromiseSupplier<T> extends Supplier<T> {
        T internalGet() throws Throwable;

        default T get() {
            try {
                return this.internalGet();
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected supplier exception", throwable);
            }
        }
    }

    public interface PromiseExecutor {
        <T> Promise<T> submit(PromiseCallable<T> callable);

        Executor executor();
    }

    protected static ThreadFactory createFactory(String prefix) {
        return new ThreadFactory() {
            AtomicLong counter = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, prefix + counter.incrementAndGet());
            }
        };
    }

    protected static PromiseExecutor BLOKING = new PromiseExecutor() {
        @Override
        public <T> Promise<T> submit(PromiseCallable<T> callable) {
            Promise<T> promise = new Promise<>();

            // inject some delay,avoid burs
            scheduler().schedule(() -> {
                try {
                    promise.complete(callable.call());
                } catch (Throwable throwable) {
                    promise.completeExceptionally(throwable);
                }
            }, ThreadLocalRandom.current().nextInt(5), TimeUnit.MILLISECONDS);

            return promise;
        }

        @Override
        public Executor executor() {
            return scheduler();
        }
    };

    protected static PromiseExecutor NONBLOCKING = new PromiseExecutor() {
        protected final ForkJoinPool delegate = new ForkJoinPool( //
                Math.max(Runtime.getRuntime().availableProcessors(), 4), //
                new ForkJoinPool.ForkJoinWorkerThreadFactory() {
                    AtomicLong count = new AtomicLong(0);

                    @Override
                    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                        ForkJoinWorkerThread thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                        thread.setName("nonblocking-pool-" + count.getAndIncrement());

                        return thread;
                    }
                },
                null,
                true
        );

        @Override
        public <T> Promise<T> submit(PromiseCallable<T> callable) {
            Promise<T> promise = new Promise<>();
            delegate.execute(() -> {
                try {
                    promise.complete(callable.call());
                } catch (Throwable throwable) {
                    promise.completeExceptionally(throwable);
                }
            });
            return promise;
        }

        @Override
        public Executor executor() {
            return this.delegate;
        }
    };

    protected static ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1, createFactory("schedule-pool-"));

    public static PromiseExecutor nonblocking() {
        return NONBLOCKING;
    }

    public static PromiseExecutor blocking() {
        return BLOKING;
    }

    public static ScheduledExecutorService scheduler() {
        return SCHEDULER;
    }

    protected static final Queue<FutureCompleteCallback> PENDING_FUTRE = new ConcurrentLinkedQueue<>();

    protected interface FutureCompleteCallback {
        Future<?> future();

        void onDone();

        void onCancle();

        default boolean callback() {
            Future<?> future = future();
            if (future == null) {
                return true;
            }

            if (future.isDone()) {
                if (future.isCancelled()) {
                    onCancle();
                } else {
                    onDone();
                }
                return true;
            }

            return false;
        }
    }

    static {
        period(() -> {
            // collect
            while (PENDING_FUTRE.removeIf(FutureCompleteCallback::callback)) {
            }
        }, 100);
    }

    public static <T> Promise<T> wrap(Future<T> future) {
        if (future instanceof Promise) {
            return (Promise<T>) (future);
        } else if (future instanceof CompletableFuture) {
            Promise<T> promise = new Promise<T>() {
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return future.cancel(mayInterruptIfRunning);
                }
            };

            ((CompletableFuture<T>) future).whenCompleteAsync((value, exception) -> {
                if (exception != null) {
                    promise.completeExceptionally(exception);
                    return;
                }

                promise.complete(value);
            }, nonblocking().executor());

            return promise;
        }

        Promise<T> promise = new Promise<T>() {
            public boolean cancel(boolean mayInterruptIfRunning) {
                return future.cancel(mayInterruptIfRunning);
            }
        };

        PENDING_FUTRE.offer(new FutureCompleteCallback() {
            @Override
            public Future<?> future() {
                return future;
            }

            @Override
            public void onDone() {
                try {
                    promise.complete(future.get());
                } catch (Throwable e) {
                    promise.completeExceptionally(e);
                }
            }

            @Override
            public void onCancle() {
                try {
                    promise.cancel(true);
                } catch (Throwable e) {
                    promise.completeExceptionally(e);
                }
            }
        });

        return promise;
    }

    public static <T> Promise<T> costly(PromiseCallable<T> callable) {
        return blocking().submit(callable);
    }

    public static Promise<?> costly(PromiseRunnable runnable) {
        return costly(() -> {
            runnable.run();
            return null;
        });
    }

    public static <T> Promise<T> light(PromiseCallable<T> callable) {
        return nonblocking().submit(callable);
    }

    public static Promise<Void> light(PromiseRunnable runnable) {
        return light(() -> {
            runnable.run();
            return null;
        });
    }

    public static <T> Promise<T> promise() {
        return new Promise<>();
    }

    public static <T> Promise<T> success(T value) {
        Promise<T> promise = new Promise<>();
        promise.complete(value);
        return promise;
    }

    public static <T> Promise<T> exceptional(PromiseSupplier<Throwable> supplier) {
        Promise<T> promise = new Promise<>();
        try {
            promise.completeExceptionally(supplier.get());
        } catch (Throwable throwable) {
            promise.completeExceptionally(new RuntimeException("fail when generating exeption", throwable));
        }
        return promise;
    }

    public static Promise<?> period(PromiseRunnable runnable, long period, PromiseConsumer<Throwable> when_exception) {
        if (runnable == null) {
            return success(null);
        }

        return Promise.wrap(scheduler() //
                .scheduleAtFixedRate(() -> {
                    try {
                        runnable.run();
                    } catch (Throwable throwable) {
                        if (when_exception != null) {
                            Promise.costly(() -> when_exception.accept(throwable)).logException();
                        } else {
                            // fallback log
                            LOGGER.error("scheudler period fail, restart:" + runnable, throwable);
                        }
                    }
                }, 0, period, TimeUnit.MILLISECONDS)
        );
    }

    public static Promise<?> period(PromiseRunnable runnable, long period) {
        return period(runnable, period, null);
    }

    public static Promise<?> delay(PromiseRunnable runnable, long delay, PromiseConsumer<Throwable> when_exception) {
        if (runnable == null) {
            return success(null);
        }

        return Promise.wrap(scheduler() //
                .schedule(() -> {
                    try {
                        runnable.run();
                    } catch (Throwable throwable) {
                        if (when_exception != null) {
                            Promise.costly(() -> when_exception.accept(throwable)).logException();
                        } else {
                            // fallback log
                            LOGGER.error("scheudler delay fail:" + runnable, throwable);
                        }
                    }
                }, delay, TimeUnit.MILLISECONDS)
        );
    }

    public static Promise<?> delay(PromiseRunnable runnable, long delay) {
        return delay(runnable, delay, null);
    }

    public static Promise<?> all(Promise<?>... promises) {
        return Arrays.stream(promises).collect(collector());
    }

    public static Collector<Promise<?>, ?, Promise<Void>> collector() {
        return Collectors.collectingAndThen(
                Collectors.reducing(
                        Promise.success(null),
                        (left, right) -> {
                            return left.transformAsync((ignore) -> right);
                        }
                ),
                (value) -> value.transform((ignore) -> null)
        );
    }

    public static <T> Collector<Promise<T>, ?, Promise<Collection<T>>> listCollector() {
        return Collectors.collectingAndThen(
                Collectors.toList(),
                (collected) -> {
                    return collected.parallelStream()
                            .collect(Promise.collector())
                            .transform((ignore) -> collected.parallelStream()
                                    .map(Promise::join)
                                    .collect(Collectors.toList())
                            );
                }
        );
    }

    public static <T> Promise<T> either(Promise<T>... promises) {
        Promise<T> promise = Promise.promise();

        // listen for success
        Arrays.stream(promises).parallel()
                .forEach((listent_to) -> {
                    listent_to.whenCompleteAsync(
                            (value, exception) -> {
                                if (exception != null) {
                                    return;
                                }

                                // racing to complete
                                promise.complete(value);

                                // then cancel other
                                Arrays.stream(promises).parallel()
                                        .forEach((other) -> other.cancel(true));
                            },
                            listent_to.usingExecutor().executor()
                    );
                });

        // when all complete,gather exception if all fail
        all(promises).exceptionally((throwable) -> {
            if (!promise.isDone()) {
                promise.completeExceptionally(throwable);
            }
            return null;
        });

        return promise;
    }

    protected Promise() {
        super();
    }

    public <R> Promise<R> transformAsync(PromiseFunction<T, Promise<R>> function) {
        Promise<T> parent = this;
        Promise<R> promise = new Promise<R>() {
            public boolean cancel(boolean mayInterruptIfRunning) {
                // 1. cancel self
                super.cancel(mayInterruptIfRunning);

                // 2. then parrent
                return parent.cancel(mayInterruptIfRunning);
            }
        };

        this.whenCompleteAsync((value, exception) -> {
            if (exception != null) {
                promise.completeExceptionally(exception);
                return;
            }

            try {
                function.apply(value).whenCompleteAsync((final_value, final_exception) -> {
                    if (final_exception != null) {
                        promise.completeExceptionally(final_exception);
                        return;
                    }

                    promise.complete(final_value);
                }, usingExecutor().executor());
            } catch (Throwable throwable) {
                LOGGER.info("value:" + value + " function:" + function);
                promise.completeExceptionally(throwable);
            }

            return;
        }, usingExecutor().executor());

        return promise;
    }

    public <R> Promise<R> transform(PromiseFunction<T, R> function) {
        return this.transformAsync((value) -> Promise.success(function.apply(value)));
    }

    public Promise<T> catching(PromiseConsumer<Throwable> when_exception) {
        // exception
        this.exceptionally((throwable) -> {
            try {
                when_exception.accept(throwable);
            } catch (Throwable exceptoin) {
                LOGGER.warn("fail catching promise", throwable);
            }

            return null;
        });

        return this;
    }

    public Promise<T> logException() {
        return this.catching((throwable) -> {
            LOGGER.error("fail promise", throwable);
        });
    }

    public Promise<T> fallback(PromiseSupplier<T> supplier) {
        Promise<T> self = this;
        Promise<T> promise = new Promise<T>() {
            public boolean cancel(boolean mayInterruptIfRunning) {
                return self.cancel(mayInterruptIfRunning);
            }
        };

        this.whenCompleteAsync((value, exception) -> {
            if (exception != null) {
                try {
                    promise.complete(supplier.get());
                } catch (Throwable throwable) {
                    promise.completeExceptionally(throwable);
                }
                return;
            }

            promise.complete(value);
            return;
        }, usingExecutor().executor());

        return promise;
    }

    public Optional<T> maybe() {
        try {
            return Optional.ofNullable(this.join());
        } catch (Throwable e) {
            return Optional.empty();
        }
    }

    public Promise<T> costly() {
        Promise<T> self = this;
        Promise<T> promise = new Promise<T>() {
            protected PromiseExecutor usingExecutor() {
                return blocking();
            }

            public boolean cancel(boolean mayInterruptIfRunning) {
                return self.cancel(mayInterruptIfRunning);
            }
        };

        this.whenCompleteAsync((value, exception) -> {
            if (exception != null) {
                promise.completeExceptionally(exception);
                return;
            }

            try {
                promise.complete(value);
            } catch (Throwable throwable) {
                promise.completeExceptionally(throwable);
            }
            return;
        }, usingExecutor().executor());

        return promise;
    }

    public Promise<T> sidekick(PromiseConsumer<T> callback) {
        this.thenAcceptAsync(callback, usingExecutor().executor());
        return this;
    }

    public Promise<T> sidekick(PromiseRunnable runnable) {
        this.thenRunAsync(runnable, usingExecutor().executor());
        return this;
    }

    public Promise<T> addListener(PromiseRunnable listener) {
        this.whenCompleteAsync((value, exception) -> {
            listener.run();
        }, usingExecutor().executor());
        return this;
    }

    public Promise<T> timeout(PromiseSupplier<T> provide_when_timeout, long timeout) {
        if (timeout < 0) {
            LOGGER.warn("timeout is " + timeout + " < 0 will not set timeout");
            return this;
        }

        Promise<T> self = this;
        Promise<T> timeout_value = new Promise<T>() {
            public boolean cancel(boolean mayInterruptIfRunning) {
                return self.cancel(mayInterruptIfRunning);
            }

        };

        // tiem out notifier
        Promise<?> when_tiemout = Promise.delay(() -> {
            try {
                timeout_value.complete(provide_when_timeout.get());
            } catch (Throwable throwable) {
                timeout_value.completeExceptionally(throwable);
            }

            // cancle this
            self.cancel(true);
        }, timeout);

        // inherit value if complte in time
        this.whenCompleteAsync((value, exception) -> {
            if (exception != null) {
                timeout_value.completeExceptionally(exception);
                return;
            }

            try {
                timeout_value.complete(value);
            } catch (Throwable e) {
                timeout_value.completeExceptionally(e);
            }

            // cancle timeout when finished
            when_tiemout.cancel(true);
        }, usingExecutor().executor());

        return timeout_value;
    }


    protected PromiseExecutor usingExecutor() {
        return nonblocking();
    }
}
