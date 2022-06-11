/*
 * Copyright 2012-2022 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.transactions.util;

import org.springframework.data.couchbase.core.TransactionalSupport;
import org.springframework.transaction.NoTransactionException;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Utility methods for transaction tests.
 */
public class TransactionTestUtil {
    private TransactionTestUtil() {}

    public static void assertInTransaction() {
        assertTrue(TransactionalSupport.checkForTransactionInThreadLocalStorage().block().isPresent());
    }

    public static void assertNotInTransaction() {
        assertFalse(TransactionalSupport.checkForTransactionInThreadLocalStorage().block().isPresent());
    }

    public static <T> Mono<T> assertInReactiveTransaction(T o) {
        System.err.println("assertInReactiveTransaction:"+o);
        return org.springframework.transaction.reactive.TransactionSynchronizationManager.forCurrentTransaction().just(o);
    }

    public static void assertNotInReactiveTransaction() {
        try {
            org.springframework.transaction.reactive.TransactionSynchronizationManager.forCurrentTransaction().block();
        }
        catch (NoTransactionException ignored) {
        }
    }

}
