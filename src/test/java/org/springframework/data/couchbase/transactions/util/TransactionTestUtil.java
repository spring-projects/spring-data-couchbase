/*
 * Copyright 2022-2025 the original author or authors
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.springframework.data.couchbase.core.TransactionalSupport;
import reactor.core.publisher.Mono;

/**
 * Utility methods for transaction tests.
 *
 * @author Graham Pople
 */
public class TransactionTestUtil {
	private TransactionTestUtil() {}

	public static void assertInTransaction() {
		assertTrue(TransactionalSupport.checkForTransactionInThreadLocalStorage().block().isPresent());
	}

	public static void assertNotInTransaction() {
		assertFalse(TransactionalSupport.checkForTransactionInThreadLocalStorage().block().isPresent());
	}

	public static <T> Mono<T> assertInReactiveTransaction(T... obj) {
		return Mono.deferContextual((ctx1) ->
				TransactionalSupport.checkForTransactionInThreadLocalStorage()
						.flatMap(ctx2 -> ctx2.isPresent() ? (obj.length>0 ? Mono.just(obj[0]) : Mono.empty()) : Mono.error(new RuntimeException("in transaction"))));
	}

	public static <T> Mono<T> assertNotInReactiveTransaction(T... obj) {
		return Mono.deferContextual((ctx1) ->
				TransactionalSupport.checkForTransactionInThreadLocalStorage()
						.flatMap(ctx2 -> !ctx2.isPresent() ? (obj.length>0 ? Mono.just(obj[0]) : Mono.empty()) : Mono.error(new RuntimeException("in transaction"))));
	}
}
