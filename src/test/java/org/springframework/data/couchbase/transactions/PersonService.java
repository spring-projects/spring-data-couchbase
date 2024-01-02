/*
 * Copyright 2022-2024 the original author or authors
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
import static org.springframework.data.couchbase.util.JavaIntegrationTests.throwSimulateFailureException;
import static org.springframework.data.couchbase.util.Util.assertInAnnotationTransaction;

import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * PersonService for tests
 *
 * @author Michael Reiche
 */
@Service // this will work in the unit tests even without @Service because of explicit loading by @SpringJUnitConfig
class PersonService {

	final CouchbaseOperations personOperations;
	final ReactiveCouchbaseOperations reactivePersonOperations;

	public PersonService(CouchbaseOperations ops, ReactiveCouchbaseOperations reactiveOps) {
		personOperations = ops;
		reactivePersonOperations = reactiveOps;
	}

	@Transactional
	public Person savePersonErrors(Person person) {
		assertInAnnotationTransaction(false);
		Person p = personOperations.insertById(Person.class).one(person);
		SimulateFailureException.throwEx("savePersonErrors");
		return p;
	}

	@Transactional
	public Person savePerson(Person person) {
		assertInAnnotationTransaction(true);
		return personOperations.insertById(Person.class).one(person);
	}

	@Transactional
	public Long countDuringTx(Person person) {
		assertInAnnotationTransaction(true);
		Person p = personOperations.insertById(Person.class).one(person);
		return personOperations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
	}

	@Transactional
	public List<CouchbasePersonTransactionIntegrationTests.EventLog> saveWithLogs(Person person) {
		assertInAnnotationTransaction(true);
		personOperations.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
				.one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(), "beforeConvert"));
		personOperations.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
				.one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(), "afterConvert"));
		personOperations.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
				.one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(), "beforeInsert"));
		personOperations.insertById(Person.class).one(person);
		personOperations.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
				.one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(), "afterInsert"));
		return personOperations.findByQuery(CouchbasePersonTransactionIntegrationTests.EventLog.class)
				.withConsistency(REQUEST_PLUS).all();
	}

	@Transactional
	public List<CouchbasePersonTransactionIntegrationTests.EventLog> saveWithErrorLogs(Person person) {
		assertInAnnotationTransaction(true);
		personOperations.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
				.one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(), "beforeConvert"));
		personOperations.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
				.one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(), "afterConvert"));
		personOperations.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
				.one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(), "beforeInsert"));
		personOperations.insertById(Person.class).one(person);
		personOperations.insertById(CouchbasePersonTransactionIntegrationTests.EventLog.class)
				.one(new CouchbasePersonTransactionIntegrationTests.EventLog(new ObjectId(), "afterInsert"));
		SimulateFailureException.throwEx("saveEventError");
		return personOperations.findByQuery(CouchbasePersonTransactionIntegrationTests.EventLog.class)
				.withConsistency(REQUEST_PLUS).all();
	}

	// org.springframework.beans.factory.NoUniqueBeanDefinitionException:
	// No qualifying bean of type 'org.springframework.transaction.TransactionManager' available: expected single
	// matching bean but found 2: reactiveCouchbaseTransactionManager,couchbaseTransactionManager
	@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	public Person declarativeSavePerson(Person person) {
		assertInAnnotationTransaction(true);
		return personOperations.insertById(Person.class).one(person);
	}

	@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	public Person declarativeSavePersonErrors(Person person) {
		assertInAnnotationTransaction(true);
		Person p = personOperations.insertById(Person.class).one(person); //
		SimulateFailureException.throwEx();
		return p;
	}

	/**
	 * to execute while ThreadReplaceloop() is running should force a retry
	 *
	 * @param person
	 * @return
	 */
	@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	public Person declarativeFindReplacePersonCallback(Person person, AtomicInteger tryCount) {
		assertInAnnotationTransaction(true);
		System.err.println("declarativeFindReplacePersonCallback try: " + tryCount.incrementAndGet());
		Person p = personOperations.findById(Person.class).one(person.id());
		ReplaceLoopThread.updateOutOfTransaction(personOperations, person, tryCount.get());
		return personOperations.replaceById(Person.class).one(p.withFirstName(person.getFirstname()));
	}

	/**
	 * The ReactiveCouchbaseTransactionManager does not retry on write-write conflict. Instead it will throw
	 * RetryTransactionException to execute while ThreadReplaceloop() is running should force a retry
	 *
	 * @param person
	 * @return
	 */
	@Transactional
	public Mono<Person> declarativeFindReplacePersonReactive(Person person, AtomicInteger tryCount) {
		assertInAnnotationTransaction(true);
		return reactivePersonOperations.findById(Person.class).one(person.id())
				.map((p) -> ReplaceLoopThread.updateOutOfTransaction(personOperations, p, tryCount.incrementAndGet()))
				.flatMap(p -> reactivePersonOperations.replaceById(Person.class).one(p.withFirstName(person.getFirstname())));
	}

	/**
	 * @param person
	 * @return
	 */
	@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	public Person declarativeFindReplacePerson(Person person, AtomicInteger tryCount) {
		assertInAnnotationTransaction(true);
		System.err.println("declarativeFindReplacePerson try: " + tryCount.incrementAndGet());
		Person p = personOperations.findById(Person.class).one(person.getId().toString());
		ReplaceLoopThread.updateOutOfTransaction(personOperations, person, tryCount.get());
		return personOperations.replaceById(Person.class).one(p.withFirstName(person.getFirstname()));
	}

	@Transactional
	public Mono<Person> declarativeSavePersonReactive(Person person) {
		assertInAnnotationTransaction(true);
		return reactivePersonOperations.insertById(Person.class).one(person);
	}

	@Transactional
	public Mono<Person> declarativeSavePersonErrorsReactive(Person person) {
		assertInAnnotationTransaction(true);
		return reactivePersonOperations.insertById(Person.class).one(person).map((pp) -> throwSimulateFailureException(pp));

	}

}
