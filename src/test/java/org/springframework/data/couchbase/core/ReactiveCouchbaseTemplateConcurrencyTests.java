package org.springframework.data.couchbase.core;

import java.util.concurrent.Semaphore;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(Config.class)
public class ReactiveCouchbaseTemplateConcurrencyTests {

    @Autowired public CouchbaseTemplate couchbaseTemplate;

    @Autowired public ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;

    @Test
    public void shouldStoreArgsForLocalThread() throws InterruptedException {
        // These will consume any args set on the current thread
        PseudoArgs<?> args1 = new PseudoArgs<>(reactiveCouchbaseTemplate, "aScope", "aCollection", null, Object.class);
        PseudoArgs<?> args2 = new PseudoArgs<>(reactiveCouchbaseTemplate, "aScope", "aCollection", null, Object.class);

        // Store args1 on this thread
        reactiveCouchbaseTemplate.setPseudoArgs(args1);

        final PseudoArgs<?>[] threadArgs = {null};

        Semaphore awaitingArgs1 = new Semaphore(0);
        Semaphore checkingArgs2 = new Semaphore(0);

        Thread t = new Thread(() -> {
            // Store args2 on separate thread
            reactiveCouchbaseTemplate.setPseudoArgs(args2);
            awaitingArgs1.release();
            try {
                // Wait to check args2
                checkingArgs2.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            threadArgs[0] = reactiveCouchbaseTemplate.getPseudoArgs();
        });
        t.start();

        // Wait for separate thread to have set args2
        awaitingArgs1.acquire();

        assertEquals(args1, reactiveCouchbaseTemplate.getPseudoArgs());
        checkingArgs2.release();
        t.join();

        assertEquals(args2, threadArgs[0]);

    }

}
