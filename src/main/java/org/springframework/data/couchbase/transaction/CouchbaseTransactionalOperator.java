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
package org.springframework.data.couchbase.transaction;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.reactive.TransactionCallback;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * The TransactionalOperator interface is another method to perform reactive transactions with Spring.
 * <p>
 * We recommend instead using a regular reactive SDK transaction, and performing Spring operations inside it.
 *
 * @author Graham Pople
 */
public class CouchbaseTransactionalOperator implements TransactionalOperator {
	private final CouchbaseCallbackTransactionManager manager;

	CouchbaseTransactionalOperator(CouchbaseCallbackTransactionManager manager) {
		this.manager = manager;
	}

	public static CouchbaseTransactionalOperator create(CouchbaseCallbackTransactionManager manager) {
		return new CouchbaseTransactionalOperator(manager);
	}

	@Override
	public <T> Mono<T> transactional(Mono<T> mono) {
		return transactional(Flux.from(mono)).singleOrEmpty();
	}

	@Override
	public <T> Flux<T> execute(TransactionCallback<T> action) throws TransactionException {
		return Flux.defer(() -> {
			TransactionDefinition def = new CouchbaseTransactionDefinition();
			return manager.executeReactive(def, action);
		});
	}
}
