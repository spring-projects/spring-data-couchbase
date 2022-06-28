/*
 * Copyright 2022 the original author or authors
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

package org.springframework.data.couchbase.transactions;

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * reactive PersonService for tests
 *
 * @author Michael Reiche
 */
class PersonServiceReactive {

	final ReactiveCouchbaseOperations personOperationsRx;
	final CouchbaseOperations personOperations;
	final TransactionalOperator transactionalOperator;

	public PersonServiceReactive(CouchbaseOperations ops, ReactiveCouchbaseOperations opsRx,
			TransactionalOperator transactionalOperator) {
		this.personOperations = ops;
		this.personOperationsRx = opsRx;
		this.transactionalOperator = transactionalOperator;
		return;
	}

	public Mono<Person> savePersonErrors(Person person) {
		return personOperationsRx.insertById(Person.class).one(person) //
				.<Person> flatMap(it -> Mono.error(new SimulateFailureException())) //
				.as(transactionalOperator::transactional);
	}

	public Mono<Person> savePerson(Person person) {
		return personOperationsRx.insertById(Person.class).one(person) //
				.flatMap(Mono::just) //
				.as(transactionalOperator::transactional);
	}

	public Mono<Long> countDuringTx(Person person) {
		return personOperationsRx.save(person) //
				.then(personOperationsRx.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count()) //
				.as(transactionalOperator::transactional);
	}

	public Flux<CouchbasePersonTransactionReactiveIntegrationTests.EventLog> saveWithLogs(Person person) {
		return Flux
				.merge(
						personOperationsRx.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(
								new ObjectId().toString(), "beforeConvert")),
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "afterConvert")),
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "beforeInsert")),
						personOperationsRx.save(person),
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "afterInsert"))) //
				.thenMany(personOperationsRx.findByQuery(CouchbasePersonTransactionReactiveIntegrationTests.EventLog.class)
						.withConsistency(REQUEST_PLUS).all()) //
				.as(transactionalOperator::transactional);
	}

	public Flux<Void> saveWithErrorLogs(Person person) {
		return Flux
				.merge(
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "beforeConvert")),
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "afterConvert")),
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "beforeInsert")),
						personOperationsRx.save(person),
						personOperationsRx
								.save(new CouchbasePersonTransactionReactiveIntegrationTests.EventLog(new ObjectId(), "afterInsert"))) //
				.<Void> flatMap(it -> Mono.error(new SimulateFailureException())) //
				.as(transactionalOperator::transactional);
	}

	// @Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	public Flux<Person> declarativeSavePerson(Person person) {
		return transactionalOperator.execute(reactiveTransaction -> personOperationsRx.save(person));
	}

	@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	public Flux<Person> declarativeSavePersonErrors(Person person) {
		Person p = personOperations.insertById(Person.class).one(person);
		Person pp = personOperations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all().get(0);
		SimulateFailureException.throwEx(); // so the following lines is not flagged as unreachable
		return Flux.just(p);
	}
}
