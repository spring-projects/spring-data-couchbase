package org.springframework.data.couchbase.core;

import rx.observers.TestSubscriber;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class AsyncUtils {

    public static void executeConcurrently(int numThreads, Callable<Void> task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);

        Collection<Callable<Void>> tasks = Collections.nCopies(numThreads, task);

        List<Future<Void>> futures = pool.invokeAll(tasks);
        for (Future future : futures) {
            future.get(numThreads, TimeUnit.SECONDS);
        }
    }

    public static <T> void awaitCompleted(TestSubscriber<T> testSubscriber) {
        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertNoValues();
        testSubscriber.assertCompleted();
    }

    public static <T> void awaitCompletedWithAnyValue(TestSubscriber<T> testSubscriber) {
        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertCompleted();
    }

    public static <T> void awaitCompletedWithValueCount(TestSubscriber<T> testSubscriber, int count) {
        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(count);
        testSubscriber.assertCompleted();
    }

    public static <T> void awaitError(TestSubscriber<T> testSubscriber, Class<? extends Throwable> throwableClazz) {
        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(throwableClazz);
        testSubscriber.assertNoValues();
    }

    public static <T> void awaitValue(TestSubscriber<T> testSubscriber, T value) {
        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValue(value);
        testSubscriber.assertCompleted();
    }
}
