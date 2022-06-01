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

package org.springframework.data.couchbase.transactions;

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import reactor.core.publisher.Mono;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
import org.springframework.data.couchbase.transaction.ReactiveTransactionsWrapper;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.error.TransactionFailedException;

/**
 * Tests for com.couchbase.transactions using
 * <li><le>couchbase reactive transaction manager via transactional operator</le> <le>couchbase non-reactive transaction
 * manager via @Transactional</le> <le>@Transactional(transactionManager =
 * BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)</le></li>
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(classes = { Config.class })
public class CouchbaseReactiveTransactionsWrapperIntegrationTests extends JavaIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory;
	@Autowired ReactivePersonRepository rxRepo;
	@Autowired PersonRepository repo;
	@Autowired CouchbaseTemplate cbTmpl;
	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;
	@Autowired CouchbaseTemplate operations;

	// if these are changed from default, then beforeEach needs to clean up separately
	String sName = "_default";
	String cName = "_default";
	private ReactiveTransactionsWrapper reactiveTransactionsWrapper;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
	}

	@AfterAll
	public static void afterAll() {
		callSuperAfterAll(new Object() {});
	}

	@BeforeEach
	public void beforeEachTest() {
		TransactionTestUtil.assertNotInTransaction();
		List<RemoveResult> rp0 = operations.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<RemoveResult> rp1 = operations.removeByQuery(Person.class).inScope(sName).inCollection(cName)
				.withConsistency(REQUEST_PLUS).all();

		List<Person> p0 = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<Person> p1 = operations.findByQuery(Person.class).inScope(sName).inCollection(cName)
				.withConsistency(REQUEST_PLUS).all();

		Person walterWhite = new Person(1, "Walter", "White");
		remove(cbTmpl, sName, cName, walterWhite.getId().toString());
		reactiveTransactionsWrapper = new ReactiveTransactionsWrapper(reactiveCouchbaseClientFactory);
	}

	@AfterEach
	public void afterEachTest() {
		TransactionTestUtil.assertNotInTransaction();
	}


	@Test
	public void wrapperReplaceWithCasConflictResolvedViaRetryReactive() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		AtomicInteger tryCount = new AtomicInteger(0);
		cbTmpl.insertById(Person.class).one(person);

		for (int i = 0; i < 50; i++) { // the transaction sometimes succeeds on the first try
			ReplaceLoopThread t = new ReplaceLoopThread(switchedPerson);
			t.start();
			tryCount.set(0);
			Mono<TransactionResult> result = reactiveTransactionsWrapper.run(ctx -> {
				System.err.println("try: " + tryCount.incrementAndGet());
				return rxCBTmpl.findById(Person.class).one(person.getId().toString()) //
						.flatMap(ppp -> rxCBTmpl.replaceById(Person.class).one(ppp)).then();
			});
			TransactionResult txResult = result.block();
			t.setStopFlag();
			if (tryCount.get() > 1) {
				break;
			}
		}
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
	}


	@Test
	public void replacePersonCBTransactionsRxTmplRollback() {
		Person person = new Person(1, "Walter", "White");
		String newName = "Walt";
		cbTmpl.insertById(Person.class).one(person);
		Mono<TransactionResult> result = reactiveTransactionsWrapper.run(ctx -> { //
			return rxCBTmpl.findById(Person.class).one(person.getId().toString()) //
					.flatMap(pp -> rxCBTmpl.replaceById(Person.class).one(pp.withFirstName(newName))).then(Mono.empty());
		});
		result.block();
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		System.err.println(pFound);
		assertEquals(newName, pFound.getFirstname());
	}

	@Test
	public void deletePersonCBTransactionsRxTmpl() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, sName, cName, person.getId().toString());
		cbTmpl.insertById(Person.class).inCollection(cName).one(person);
		Mono<TransactionResult> result = reactiveTransactionsWrapper.run(ctx -> { // get the ctx
			return rxCBTmpl.removeById(Person.class).inCollection(cName).oneEntity(person).then();
		});
		result.block();
		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertNull(pFound, "Should not have found " + pFound);
	}

	@Test
	public void deletePersonCBTransactionsRxTmplFail() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, sName, cName, person.getId().toString());
		cbTmpl.insertById(Person.class).inCollection(cName).one(person);
		Mono<TransactionResult> result = reactiveTransactionsWrapper.run(ctx -> { // get the ctx
			return rxCBTmpl.removeById(Person.class).inCollection(cName).oneEntity(person)
					.then(rxCBTmpl.removeById(Person.class).inCollection(cName).oneEntity(person)).then();
		});
		assertThrowsWithCause(result::block, TransactionFailedException.class, DataRetrievalFailureException.class);
		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertEquals(pFound, person, "Should have found " + person);
	}

	@Test
	public void deletePersonCBTransactionsRxRepo() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, sName, cName, person.getId().toString());
		repo.withCollection(cName).save(person);
		Mono<TransactionResult> result = reactiveTransactionsWrapper.run(ctx -> { // get the ctx
			return rxRepo.withCollection(cName).delete(person).then();
		});
		result.block();
		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertNull(pFound, "Should not have found " + pFound);
	}

	@Test
	public void deletePersonCBTransactionsRxRepoFail() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, sName, cName, person.getId().toString());
		repo.withCollection(cName).save(person);
		Mono<TransactionResult> result = reactiveTransactionsWrapper.run(ctx -> { // get the ctx
			return rxRepo.withCollection(cName).findById(person.getId().toString())
					.flatMap(pp -> rxRepo.withCollection(cName).delete(pp).then(rxRepo.withCollection(cName).delete(pp))).then();
		});
		assertThrowsWithCause(result::block, TransactionFailedException.class, DataRetrievalFailureException.class);
		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertEquals(pFound, person, "Should have found " + person + " instead of " + pFound);
	}

	@Test
	public void findPersonCBTransactions() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, sName, cName, person.getId().toString());
		cbTmpl.insertById(Person.class).inScope(sName).inCollection(cName).one(person);
		List<Object> docs = new LinkedList<>();
		Query q = Query.query(QueryCriteria.where("meta().id").eq(person.getId()));
		Mono<TransactionResult> result = reactiveTransactionsWrapper.run(ctx -> {
			return rxCBTmpl.findByQuery(Person.class)
					.inScope(sName).inCollection(cName).matching(q).withConsistency(REQUEST_PLUS).one().doOnSuccess(doc -> {
						System.err.println("doc: " + doc);
						docs.add(doc);
					});
		});
		result.block();
		assertFalse(docs.isEmpty(), "Should have found " + person);
		for (Object o : docs) {
			assertEquals(o, person, "Should have found " + person + " instead of " + o);
		}
	}

	@Test
	public void insertPersonRbCBTransactions() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, sName, cName, person.getId().toString());
		Mono<TransactionResult> result = reactiveTransactionsWrapper
				.run(ctx -> rxCBTmpl.insertById(Person.class).inScope(sName).inCollection(cName).one(person)
						.<Person> flatMap(it -> Mono.error(new SimulateFailureException())));
		assertThrowsWithCause(() -> result.block(), TransactionFailedException.class, SimulateFailureException.class);
		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertNull(pFound, "Should not have found " + pFound);
	}

	@Test
	public void replacePersonRbCBTransactions() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, sName, cName, person.getId().toString());
		cbTmpl.insertById(Person.class).inScope(sName).inCollection(cName).one(person);
		Mono<TransactionResult> result = reactiveTransactionsWrapper.run(ctx -> //
		rxCBTmpl.findById(Person.class).inScope(sName).inCollection(cName).one(person.getId().toString()) //
				.flatMap(pFound -> rxCBTmpl.replaceById(Person.class).inScope(sName).inCollection(cName)
						.one(pFound.withFirstName("Walt")))
				.<Person> flatMap(it -> Mono.error(new SimulateFailureException())));
		assertThrowsWithCause(() -> result.block(), TransactionFailedException.class, SimulateFailureException.class);
		Person pFound = cbTmpl.findById(Person.class).inScope(sName).inCollection(cName).one(person.getId().toString());
		assertEquals(person, pFound, "Should have found " + person + " instead of " + pFound);
	}

	@Test
	public void findPersonSpringTransactions() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, sName, cName, person.getId().toString());
		cbTmpl.insertById(Person.class).inScope(sName).inCollection(cName).one(person);
		List<Object> docs = new LinkedList<>();
		Query q = Query.query(QueryCriteria.where("meta().id").eq(person.getId()));
		Mono<TransactionResult> result = reactiveTransactionsWrapper.run(ctx -> rxCBTmpl.findByQuery(Person.class)
				.inScope(sName).inCollection(cName).matching(q).one().doOnSuccess(r -> docs.add(r)));
		result.block();
		assertFalse(docs.isEmpty(), "Should have found " + person);
		for (Object o : docs) {
			assertEquals(o, person, "Should have found " + person);
		}
	}

	private class ReplaceLoopThread extends Thread {
		AtomicBoolean stop = new AtomicBoolean(false);
		Person person;
		int maxIterations = 100;

		public ReplaceLoopThread(Person person, int... iterations) {
			this.person = person;
			if (iterations != null && iterations.length == 1) {
				this.maxIterations = iterations[0];
			}
		}

		public void run() {
			for (int i = 0; i < maxIterations && !stop.get(); i++) {
				sleepMs(10);
				try {
					// note that this does not go through spring-data, therefore it does not have the @Field , @Version etc.
					// annotations processed so we just check getFirstname().equals()
					// switchedPerson has version=0, so it doesn't check CAS
					couchbaseClientFactory.getBucket().defaultCollection().replace(person.getId().toString(), person);
					System.out.println("********** replace thread: " + i + " success");
				} catch (Exception e) {
					System.out.println("********** replace thread: " + i + " " + e.getClass().getName());
				}
			}

		}

		public void setStopFlag() {
			stop.set(true);
		}
	}

	void remove(CouchbaseTemplate template, String scope, String collection, String id) {
		remove(template.reactive(), scope, collection, id);
	}

	void remove(ReactiveCouchbaseTemplate template, String scope, String collection, String id) {
		try {
			template.removeById(Person.class).inScope(scope).inCollection(collection).one(id).block();
			List<Person> ps = template.findByQuery(Person.class).inScope(scope).inCollection(collection)
					.withConsistency(REQUEST_PLUS).all().collectList().block();
		} catch (DocumentNotFoundException | DataRetrievalFailureException nfe) {
			System.out.println(id + " : " + "DocumentNotFound when deleting");
		}
	}
}
