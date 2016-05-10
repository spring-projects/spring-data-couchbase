package org.springframework.data.couchbase.core;

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
}
