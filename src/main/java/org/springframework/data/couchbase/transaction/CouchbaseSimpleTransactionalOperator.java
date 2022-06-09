/*
 * Copyright 2021 the original author or authors
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

import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.reactive.TransactionCallback;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The simplest possible implementation of TransactionalOperator.
 */
public class CouchbaseSimpleTransactionalOperator implements TransactionalOperator {
	private final CouchbaseSimpleCallbackTransactionManager manager;

	// todo gpx static or ctor?
	public CouchbaseSimpleTransactionalOperator(CouchbaseSimpleCallbackTransactionManager manager) {
		this.manager = manager;
	}

	@Override
	public <T> Mono<T> transactional(Mono<T> mono) {
		return transactional(Flux.from(mono))
				.singleOrEmpty();
	}

	@Override
	public <T> Flux<T> execute(TransactionCallback<T> action) throws TransactionException {
		return Flux.defer(() -> {
			TransactionDefinition def = new CouchbaseTransactionDefinition();
			return manager.executeReactive(def, action);
		});
	}
}
