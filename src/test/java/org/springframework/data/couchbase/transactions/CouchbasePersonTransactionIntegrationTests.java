/*
 * Copyright 2012-2021 the original author or authors
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import lombok.Data;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.transaction.ClientSession;
import org.springframework.data.couchbase.transaction.ClientSessionOptions;
import org.springframework.data.couchbase.transaction.CouchbaseCallbackTransactionManager;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionDefinition;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionManager;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseResourceHolder;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
import org.springframework.data.couchbase.transaction.TransactionsWrapper;
import org.springframework.data.couchbase.transaction.interceptor.CouchbaseTransactionInterceptor;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.config.TransactionManagementConfigUtils;
import org.springframework.transaction.interceptor.BeanFactoryTransactionAttributeSourceAdvisor;
import org.springframework.transaction.interceptor.TransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.reactive.TransactionContextManager;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.RemoveOptions;

/**
 * Tests for com.couchbase.transactions using
 * <li><le>couchbase reactive transaction manager via transactional operator</le> <le>couchbase non-reactive transaction
 * manager via @Transactional</le> <le>@Transactional(transactionManager =
 * BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)</le></li>
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(CouchbasePersonTransactionIntegrationTests.Config.class)
public class CouchbasePersonTransactionIntegrationTests extends JavaIntegrationTests {

	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory;
	@Autowired ReactiveCouchbaseTransactionManager reactiveCouchbaseTransactionManager;
	//@Autowired CouchbaseTransactionManager couchbaseTransactionManager;
	@Autowired ReactivePersonRepository rxRepo;
	@Autowired PersonRepository repo;
	@Autowired CouchbaseTemplate cbTmpl;
	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;
	/* DO NOT @Autowired - it will result in no @Transactional annotation behavior */ PersonService personService;
	@Autowired CouchbaseTemplate operations;

	static GenericApplicationContext context;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
		context = new AnnotationConfigApplicationContext(CouchbasePersonTransactionIntegrationTests.Config.class,
				PersonService.class, CouchbasePersonTransactionIntegrationTests.TransactionInterception.class);
	}

	@AfterAll
	public static void afterAll() {
		callSuperAfterAll(new Object() {});
		context.close();
	}

	@BeforeEach
	public void beforeEachTest() {
		personService = context.getBean(PersonService.class); // getting it via autowired results in no @Transactional
		// Skip this as we just one to track TransactionContext
		operations.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all(); // doesn't work???
		operations.removeByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all();
		List<Person> p = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<EventLog> e = operations.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all();

		Person walterWhite = new Person(1, "Walter", "White");
		try {
			couchbaseClientFactory.getBucket().defaultCollection().remove(walterWhite.getId().toString());
		} catch (Exception ex) {
			// System.err.println(ex);
		}
	}

	/* Not used in this class.  The class itself is not @Transaction

	List<AfterTransactionAssertion<? extends Persistable<?>>> assertionList;
	
		@BeforeTransaction
		public void beforeTransaction() {
			System.err.println("BeforeTransaction");
			assertionList = new ArrayList<>();
		}
	
		@AfterTransaction
		public void afterTransaction() {
			System.err.println("AfterTransaction");
			if (assertionList == null) {
				return;
			}
			assertionList.forEach(it -> {
				Person p = (Person) (operations.findById(it.getPersistable().getClass()).one(it.getId().toString()));
				boolean isPresent = p != null;
				System.err.println(("isPresent: " + isPresent + " shouldBePresent: " + it.shouldBePresent()));
				assertThat(isPresent).isEqualTo(it.shouldBePresent())
						.withFailMessage(String.format("After transaction entity %s should %s.", it.getPersistable(),
								it.shouldBePresent() ? "be present" : "NOT be present"));
			});
		}
	
		private AfterTransactionAssertion assertAfterTransaction(Person p) {
			AfterTransactionAssertion<Person> assertion = new AfterTransactionAssertion<>(p);
			if (assertionList != null) {
				assertionList.add(assertion);
			}
			return assertion;
		}
	 */

	@Test
	public void shouldRollbackAfterException() {
		Person p = new Person(null, "Walter", "White");
		assertThrows(SimulateFailureException.class, () -> personService.savePersonErrors(p));
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethod() {
		Person p = new Person(null, "Walter", "White");
		assertThrows(SimulateFailureException.class, () -> personService.declarativeSavePersonErrors(p));
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethodReactive() {
		Person p = new Person(null, "Walter", "White");
		assertThrows(SimulateFailureException.class, () -> personService.declarativeSavePersonErrorsReactive(p).block());
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	public void commitShouldPersistTxEntries() {
		Person p = new Person(null, "Walter", "White");
		Person s = personService.savePerson(p);
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	public void commitShouldPersistTxEntriesOfTxAnnotatedMethod() {
		Person p = new Person(null, "Walter", "White");
		Person s = personService.declarativeSavePerson(p);
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	public void commitShouldPersistTxEntriesOfTxAnnotatedMethodReactive() {
		Person p = new Person(null, "Walter", "White");
		Person s = personService.declarativeSavePersonReactive(p).block();
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	public void commitShouldPersistTxEntriesAcrossCollections() {
		List<EventLog> persons = personService.saveWithLogs(new Person(null, "Walter", "White"));
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
		Long countEvents = operations.count(new Query(), EventLog.class); //
		assertEquals(4, countEvents, "should have saved and found 4");
	}

	@Test
	public void rollbackShouldAbortAcrossCollections() {
		assertThrows(SimulateFailureException.class,
				() -> personService.saveWithErrorLogs(new Person(null, "Walter", "White")));
		List<Person> persons = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		assertEquals(0, persons.size(), "should have done roll back and left 0 entries");
		List<EventLog> events = operations.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all(); //
		assertEquals(0, events.size(), "should have done roll back and left 0 entries");
	}

	@Test
	public void countShouldWorkInsideTransaction() {
		Long count = personService.countDuringTx(new Person(null, "Walter", "White"));
		assertEquals(1, count, "should have counted 1 during tx");
	}

	@Test
	public void emitMultipleElementsDuringTransaction() {
		List<EventLog> docs = personService.saveWithLogs(new Person(null, "Walter", "White"));
		assertEquals(4, docs.size(), "should have found 4 eventlogs");
	}

	@Test
	public void errorAfterTxShouldNotAffectPreviousStep() {
		Person p = personService.savePerson(new Person(null, "Walter", "White"));
		// todo gp user shouldn't be getting exposed to TransactionOperationFailedException
		assertThrows(TransactionOperationFailedException.class, () -> personService.savePerson(p));
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	/**
	 * This will appear to work even if replaceById does not use a transaction.
	 */
	@Test
	@Disabled
	public void replacePersonCBTransactionsRxTmpl() {
		Person person = new Person(1, "Walter", "White");
		cbTmpl.insertById(Person.class).one(person);
		Mono<TransactionResult> result = this.couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> { // get the ctx
			ClientSession clientSession = couchbaseClientFactory
					.getSession(ClientSessionOptions.builder().causallyConsistent(true).build(), ctx);
			ReactiveCouchbaseResourceHolder resourceHolder = new ReactiveCouchbaseResourceHolder(clientSession,
					reactiveCouchbaseClientFactory);
			Mono<TransactionSynchronizationManager> sync = TransactionContextManager.currentContext()
					.map(TransactionSynchronizationManager::new).flatMap(synchronizationManager -> {
						synchronizationManager.bindResource(reactiveCouchbaseClientFactory.getCluster().block(), resourceHolder);
						prepareSynchronization(synchronizationManager, null, new CouchbaseTransactionDefinition());
						return rxCBTmpl.findById(Person.class).one(person.getId().toString()) //
								.flatMap((pp) -> rxCBTmpl.replaceById(Person.class).one(pp)) //
								.then(Mono.just(synchronizationManager)); // tx
					});
			return sync.contextWrite(TransactionContextManager.getOrCreateContext())
					.contextWrite(TransactionContextManager.getOrCreateContextHolder()).then();
		});

		result.block();
		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
		assertEquals(person, pFound, "should have found expected");
	}

	@Test
	public void insertPersonCBTransactionsRxTmplRollback() {
		Person person = new Person(1, "Walter", "White");
		try {
			rxCBTmpl.removeById(Person.class).one(person.getId().toString());
		} catch(DocumentNotFoundException dnfe){}
		Mono<TransactionResult> result = couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> { // get the ctx

			ClientSession clientSession = couchbaseClientFactory
					.getSession(ClientSessionOptions.builder().causallyConsistent(true).build(), ctx);
			ReactiveCouchbaseResourceHolder resourceHolder = new ReactiveCouchbaseResourceHolder(clientSession,
					reactiveCouchbaseClientFactory);
			Mono<TransactionSynchronizationManager> sync = TransactionContextManager.currentContext()
					.map(TransactionSynchronizationManager::new).flatMap(synchronizationManager -> {
						synchronizationManager.bindResource(reactiveCouchbaseClientFactory.getCluster().block(), resourceHolder);
						prepareSynchronization(synchronizationManager, null, new CouchbaseTransactionDefinition());
						// execute the transaction (insertById, SimulateFailure), insertById() will fetch the ctx from the context
						return rxCBTmpl.insertById(Person.class).one(person).then(Mono.error(new SimulateFailureException())); // tx
					});
			return sync.contextWrite(TransactionContextManager.getOrCreateContext())
					.contextWrite(TransactionContextManager.getOrCreateContextHolder()).then();

		});
		assertThrowsCause(TransactionFailedException.class, SimulateFailureException.class, (ignore) -> {
			result.block();
			return null;
		});
		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
		assertNull(pFound, "insert should have been rolled back");
	}

	@Test
	public void insertTwicePersonCBTransactionsRxTmplRollback() {
		Person person = new Person(1, "Walter", "White");
		sleepMs(1000);
		Mono<TransactionResult> result = couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> { // get the ctx
			ClientSession clientSession = couchbaseClientFactory
					.getSession(ClientSessionOptions.builder().causallyConsistent(true).build(), ctx);
			ReactiveCouchbaseResourceHolder resourceHolder = new ReactiveCouchbaseResourceHolder(clientSession,
					reactiveCouchbaseClientFactory);
			Mono<TransactionSynchronizationManager> sync = TransactionContextManager.currentContext()
					.map(TransactionSynchronizationManager::new).flatMap(synchronizationManager -> {
						synchronizationManager.bindResource(reactiveCouchbaseClientFactory.getCluster().block(), resourceHolder);
						prepareSynchronization(synchronizationManager, null, new CouchbaseTransactionDefinition());
						return rxCBTmpl.insertById(Person.class).one(person) //
								.flatMap((ppp) -> rxCBTmpl.insertById(Person.class).one(ppp)) //
								.then(Mono.just(synchronizationManager)); // tx
					});
			return sync.contextWrite(TransactionContextManager.getOrCreateContext())
					.contextWrite(TransactionContextManager.getOrCreateContextHolder()).then();
		});
		assertThrowsCause(TransactionFailedException.class, DuplicateKeyException.class, (ignore) -> {
			result.block();
			return null;
		});
		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
		assertNull(pFound, "insert should have been rolled back");
	}

	@Test
	public void replaceWithCasConflictResolvedViaRetry() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		AtomicBoolean stop = new AtomicBoolean(false);
		Thread t = new ReplaceLoopThread(stop, switchedPerson);
		t.start();
		cbTmpl.insertById(Person.class).one(person);

		AtomicInteger tryCount = new AtomicInteger(0);
		Mono<TransactionResult> result = couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> { // get the ctx
			// see TransactionalOperatorImpl.transactional().
			ClientSession clientSession = couchbaseClientFactory
					.getSession(ClientSessionOptions.builder().causallyConsistent(true).build(), ctx);
			ReactiveCouchbaseResourceHolder resourceHolder = new ReactiveCouchbaseResourceHolder(clientSession,
					reactiveCouchbaseClientFactory);
			Mono<TransactionSynchronizationManager> sync = TransactionContextManager.currentContext()
					.map(TransactionSynchronizationManager::new).flatMap(synchronizationManager -> {
						synchronizationManager.bindResource(reactiveCouchbaseClientFactory.getCluster().block(), resourceHolder);
						prepareSynchronization(synchronizationManager, null, new CouchbaseTransactionDefinition());
						return rxCBTmpl.findById(Person.class).one(person.getId().toString()) //
								.flatMap((ppp) -> {
									tryCount.getAndIncrement();
									System.err.println("===== ATTEMPT : " + tryCount.get() + " =====");
									return Mono.just(ppp);
								})//
								.flatMap((ppp) -> rxCBTmpl.replaceById(Person.class).one(ppp)) //
								.then(Mono.just(synchronizationManager)); // tx
					});

			return sync.contextWrite(TransactionContextManager.getOrCreateContext())
					.contextWrite(TransactionContextManager.getOrCreateContextHolder()).then();
		});

		result.block();

		stop.set(true);
		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
		assertTrue(tryCount.get() > 1, "should have been more than one try ");
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
	}

	@Test
	public void wrapperReplaceWithCasConflictResolvedViaRetry() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		AtomicInteger tryCount = new AtomicInteger(0);

		for (int i = 0; i < 10; i++) { // the transaction sometimes succeeds on the first try
			AtomicBoolean stop = new AtomicBoolean(false);
			Thread t = new ReplaceLoopThread(stop, switchedPerson);
			t.start();
			cbTmpl.insertById(Person.class).one(person);
			tryCount.set(0);
			TransactionsWrapper transactionsWrapper = new TransactionsWrapper(reactiveCouchbaseClientFactory);
			Mono<TransactionResult> result = transactionsWrapper.reactive(ctx -> {
				System.err.println("try: " + tryCount.incrementAndGet());
				return rxCBTmpl.findById(Person.class).one(person.getId().toString()) //
						.flatMap((ppp) -> rxCBTmpl.replaceById(Person.class).one(ppp)).then();
			});
			TransactionResult txResult = result.block();
			stop.set(true);
			System.out.println("txResult: " + txResult);
			if (tryCount.get() > 1) {
				break;
			}
		}
		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
	}

	/**
	 * This does process retries - by CallbackTransactionManager.execute() -> transactions.run() -> executeTransaction()
	 * -> retryWhen. The CallbackTransactionManager only finds the resources in the Thread - it doesn't find it in the
	 * context. It might be nice to use the context for both - but I'm not sure if that is possible - mostly due to
	 * ExecutableFindById.one() calling reactive.one().block() instead of returning a publisher which could have
	 * .contextWrite() called on it.
	 */
	@Test
	public void replaceWithCasConflictResolvedViaRetryAnnotatedCallback() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		AtomicBoolean stop = new AtomicBoolean(false);
		Thread t = new ReplaceLoopThread(stop, switchedPerson);
		t.start();
		cbTmpl.insertById(Person.class).one(person);
		AtomicInteger tryCount = new AtomicInteger(0);
		Person p = personService.declarativeFindReplacePersonCallback(person, tryCount);
		stop.set(true);
		System.out.println("person: " + p);
		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
	}

	/**
	 * This fails with TransactionOperationFailed {ec:FAIL_CAS_MISMATCH, retry:true, autoRollback:true}. I don't know why
	 * it isn't retried. This seems like it is due to the functioning of AbstractReactiveTransactionManager
	 */
	@Test
	public void replaceWithCasConflictResolvedViaRetryAnnotatedReactive() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		AtomicBoolean stop = new AtomicBoolean(false);
		Thread t = new ReplaceLoopThread(stop, switchedPerson);
		t.start();
		cbTmpl.insertById(Person.class).one(person);
		AtomicInteger tryCount = new AtomicInteger(0);
		Person p = personService.declarativeFindReplacePersonReactive(person, tryCount).block();
		stop.set(true);
		System.out.println("person: " + p);
		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
	}

	@Test
	/**
	 * This fails with TransactionOperationFailed {ec:FAIL_CAS_MISMATCH, retry:true, autoRollback:true}. I don't know why
	 * it isn't retried. This seems like it is due to the functioning of AbstractPlatformTransactionManager
	 */
	public void replaceWithCasConflictResolvedViaRetryAnnotated() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		AtomicBoolean stop = new AtomicBoolean(false);
		Thread t = new ReplaceLoopThread(stop, switchedPerson);
		t.start();
		cbTmpl.insertById(Person.class).one(person);
		AtomicInteger tryCount = new AtomicInteger(0);
		Person p = personService.declarativeFindReplacePerson(person, tryCount);
		stop.set(true);
		System.out.println("person: " + p);
		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
	}

	private class ReplaceLoopThread extends Thread {
		AtomicBoolean stop;
		Person person;

		public ReplaceLoopThread(AtomicBoolean stop, Person person) {
			this.stop = stop;
			this.person = person;
		}

		public void run() {
			for (int i = 0; i < 100 && !stop.get(); i++) {
				sleepMs(5);
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
	}
	/*
		@Test
	public void replacePersonCBTransactionsRxTmplRollback() {
		Person person = new Person(1, "Walter", "White");
		String newName = "Walt";
		rxCBTmpl.insertById(Person.class).one(person).block();
		sleepMs(1000);
		Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
			// can we take the ReactiveTransactionAttemptContext ctx and save it in the context?
			ClientSession clientSession = couchbaseClientFactory
					.getSession(ClientSessionOptions.builder().causallyConsistent(true).build(), ctx);
			CouchbaseResourceHolder resourceHolder = new CouchbaseResourceHolder(clientSession, couchbaseClientFactory);
	
	// I think this needs to happen within the couchbaseClientFactory.getCluster().reactive().transactions().run() call - or equivalent.
	
	// this currentContext() call is going to create a new ctx, and store the acr.  Will it get uses in syncFlatMap()
	// below?  Should the ctx be created in the above call to couchbaseClientFactory.getCluster().reactive().transactions().run()?
	// How does this work in savePerson etc?
	// is there means for just getting the currentContext() without creating it? 
	Mono<TransactionSynchronizationManager> sync = TransactionContextManager.currentContext().map(TransactionSynchronizationManager::new)
			.flatMap(synchronizationManager -> {
				synchronizationManager.bindResource(couchbaseClientFactory, resourceHolder);  // is this binding to the right syncManager?
				prepareSynchronization(synchronizationManager, null, new CouchbaseTransactionDefinition());
				return Mono.just(synchronizationManager);
			}).contextWrite(TransactionContextManager.getOrCreateContext())
			.contextWrite(TransactionContextManager.getOrCreateContextHolder());
	
	
			return sync.flatMap( (ignore) -> {
		System.out.println("TSM: "+ignore);
		return rxCBTmpl.findById(Person.class)
				.one(person.getId().toString()); }) // need to get the TSM context in the one() calls.
			.flatMap(pp ->  rxCBTmpl.replaceById(Person.class).one(pp.withFirstName("Walt"))).then(Mono.empty()));
	}));
	
	
		result.block();
		// assertThrows(TransactionFailedException.class, () -> result.block());
		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
		System.err.println(pFound);
		assertEquals(person.getFirstname(), pFound.getFirstname());
		}
	
		*/

	/*
	@Test
	public void deletePersonCBTransactionsRxTmpl() {
		Person person = new Person(1, "Walter", "White");
		rxCBTmpl.insertById(Person.class).one(person).block();
		sleepMs(1000);
		Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
			return rxCBTmpl.removeById(Person.class).transaction(new CouchbaseStuffHandle(reactiveCouchbaseTransactionManager)).one(person.getId().toString())
					.then();
		}));
		result.block();
		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
		assertNull(pFound, "Should not have found " + pFound);
	}
	
	  @Test
	  public void deletePersonCBTransactionsRxTmpl() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    rxCBTmpl.insertById(Person.class).inCollection(cName).one(person).block();
	
	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxCBTmpl.removeById(Person.class).inCollection(cName).transaction(ctx).one(person.getId().toString())
	          .then();
	    }));
	    result.block();
	    Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
	    assertNull(pFound, "Should not have found " + pFound);
	  }
	
	  @Test
	  public void deletePersonCBTransactionsRxTmplFail() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    cbTmpl.insertById(Person.class).inCollection(cName).one(person);
	
	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxCBTmpl.removeById(Person.class).inCollection(cName).transaction(ctx).one(person.getId().toString())
	          .then(rxCBTmpl.removeById(Person.class).inCollection(cName).transaction(ctx).one(person.getId().toString()))
	          .then();
	    }));
	    assertThrows(TransactionFailedException.class, result::block);
	    Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	    assertEquals(pFound, person, "Should have found " + person);
	  }
	
	  //  RxRepo ////////////////////////////////////////////////////////////////////////////////////////////
	
	  @Test
	  public void deletePersonCBTransactionsRxRepo() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    rxRepo.withCollection(cName).save(person).block();
	
	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxRepo.withCollection(cName).withTransaction(ctx).deleteById(person.getId().toString()).then();
	    }));
	    result.block();
	    Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	    assertNull(pFound, "Should not have found " + pFound);
	  }
	
	  @Test
	  public void deletePersonCBTransactionsRxRepoFail() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    rxRepo.withCollection(cName).save(person).block();
	
	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxRepo.withCollection(cName).withTransaction(ctx).deleteById(person.getId().toString())
	          .then(rxRepo.withCollection(cName).withTransaction(ctx).deleteById(person.getId().toString())).then();
	    }));
	    assertThrows(TransactionFailedException.class, result::block);
	    Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	    assertEquals(pFound, person, "Should have found " + person);
	  }
	
	  @Test
	  public void findPersonCBTransactions() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    cbTmpl.insertById(Person.class).inCollection(cName).one(person);
	    List<Object> docs = new LinkedList<>();
	    Query q = Query.query(QueryCriteria.where("meta().id").eq(person.getId()));
	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxCBTmpl.findByQuery(Person.class).inCollection(cName).matching(q).transaction(ctx).one().map(doc -> {
	        docs.add(doc);
	        return doc;
	      }).then();
	    }));
	    result.block();
	    assertFalse(docs.isEmpty(), "Should have found " + person);
	    for (Object o : docs) {
	      assertEquals(o, person, "Should have found " + person);
	    }
	  }
	
	  @Test
	  // @Transactional
	  // Failed to retrieve PlatformTransactionManager for @Transactional test:
	  public void insertPersonRbCBTransactions() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	
	    Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
	      return rxCBTmpl.insertById(Person.class).inCollection(cName).transaction(ctx).one(person)
	          .<Person> flatMap(it -> Mono.error(new PoofException())).then();
	    });
	
	    try {
	      result.block();
	    } catch (TransactionFailedException e) {
	      e.printStackTrace();
	      if (e.getCause() instanceof PoofException) {
	        Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	        assertNull(pFound, "Should not have found " + pFound);
	        return;
	      } else {
	        e.printStackTrace();
	      }
	    }
	    throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
	  }
	
	  @Test
	  // @Transactional // TODO @Transactional does nothing. Transaction is handled by transactionalOperator
	  // Failed to retrieve PlatformTransactionManager for @Transactional test:
	  public void replacePersonRbCBTransactions() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    cbTmpl.insertById(Person.class).inCollection(cName).one(person);
	    Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
	      return rxCBTmpl.findById(Person.class).inCollection(cName).transaction(ctx).one(person.getId().toString())
	          .flatMap(pFound -> rxCBTmpl.replaceById(Person.class).inCollection(cName).transaction(ctx)
	              .one(pFound.withFirstName("Walt")))
	          .<Person> flatMap(it -> Mono.error(new PoofException())).then();
	    });
	
	    try {
	      result.block();
	    } catch (TransactionFailedException e) {
	      if (e.getCause() instanceof PoofException) {
	        Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	        assertEquals(person, pFound, "Should have found " + person);
	        return;
	      } else {
	        e.printStackTrace();
	      }
	    }
	    throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
	  }
	
	  @Test
	  public void findPersonSpringTransactions() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    cbTmpl.insertById(Person.class).inCollection(cName).one(person);
	    List<Object> docs = new LinkedList<>();
	    Query q = Query.query(QueryCriteria.where("meta().id").eq(person.getId()));
	    Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
	      return rxCBTmpl.findByQuery(Person.class).inCollection(cName).matching(q).transaction(ctx).one().map(doc -> {
	        docs.add(doc);
	        return doc;
	      }).then();
	    });
	    result.block();
	    assertFalse(docs.isEmpty(), "Should have found " + person);
	    for (Object o : docs) {
	      assertEquals(o, person, "Should have found " + person);
	    }
	  }
	*/
	void remove(Collection col, String id) {
		remove(col.reactive(), id);
	}

	void remove(ReactiveCollection col, String id) {
		try {
			col.remove(id, RemoveOptions.removeOptions().timeout(Duration.ofSeconds(10))).block();
		} catch (DocumentNotFoundException nfe) {
			System.out.println(id + " : " + "DocumentNotFound when deleting");
		}
	}

	void remove(CouchbaseTemplate template, String collection, String id) {
		remove(template.reactive(), collection, id);
	}

	void remove(ReactiveCouchbaseTemplate template, String collection, String id) {
		try {
			template.removeById(Person.class).inCollection(collection).one(id).block();
			System.out.println("removed " + id);
		} catch (DocumentNotFoundException | DataRetrievalFailureException nfe) {
			System.out.println(id + " : " + "DocumentNotFound when deleting");
		}
	}

	private static void prepareSynchronization(TransactionSynchronizationManager synchronizationManager,
			ReactiveTransaction status, TransactionDefinition definition) {

		// if (status.isNewSynchronization()) {
		synchronizationManager.setActualTransactionActive(false /*status.hasTransaction()*/);
		synchronizationManager.setCurrentTransactionIsolationLevel(
				definition.getIsolationLevel() != TransactionDefinition.ISOLATION_DEFAULT ? definition.getIsolationLevel()
						: null);
		synchronizationManager.setCurrentTransactionReadOnly(definition.isReadOnly());
		synchronizationManager.setCurrentTransactionName(definition.getName());
		synchronizationManager.initSynchronization();
		// }
	}

	void assertThrowsCause(Class<?> exceptionClass, Class<?> causeClass, Function<?, ?> function) {
		try {
			function.apply(null);
		} catch (Throwable tfe) {
			System.err.println("Exception: " + tfe + " causedBy: " + tfe.getCause());
			if (tfe.getClass().isAssignableFrom(exceptionClass)) {
				if (tfe.getCause() != null && tfe.getCause().getClass().isAssignableFrom(causeClass)) {
					System.err.println("thrown exception was: " + tfe + " cause: " + tfe.getCause());
					return;
				}
			}
			throw new RuntimeException("expected " + exceptionClass + " with cause " + causeClass + " but got " + tfe);
		}
		throw new RuntimeException("expected " + exceptionClass + " with cause " + causeClass + " nothing was thrown");
	}

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	@EnableReactiveCouchbaseRepositories("org.springframework.data.couchbase")
	static class Config extends AbstractCouchbaseConfiguration {

		@Override
		public String getConnectionString() {
			return connectionString();
		}

		@Override
		public String getUserName() {
			return config().adminUsername();
		}

		@Override
		public String getPassword() {
			return config().adminPassword();
		}

		@Override
		public String getBucketName() {
			return bucketName();
		}


		/*
				beforeAll creates a PersonService bean in the applicationContext
		
				context = new AnnotationConfigApplicationContext(CouchbasePersonTransactionIntegrationTests.Config.class,
				PersonService.class);
		
				@Bean("personService")
						PersonService getPersonService(CouchbaseOperations ops, CouchbaseTransactionManager mgr,
																					 ReactiveCouchbaseOperations opsRx, ReactiveCouchbaseTransactionManager mgrRx) {
							return new PersonService(ops, mgr, opsRx, mgrRx);
						}
		*/

	}

	@Data
	// @AllArgsConstructor
	static class EventLog {
		public EventLog() {}

		public EventLog(ObjectId oid, String action) {
			this.id = oid.toString();
			this.action = action;
		}

		public EventLog(String id, String action) {
			this.id = id;
			this.action = action;
		}

		String id;
		String action;

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("EventLog : {\n");
			sb.append("  id : " + getId());
			sb.append(", action: " + action);
			return sb.toString();
		}
	}

	@Configuration(proxyBeanMethods = false)
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	static class TransactionInterception {

		@Bean
		@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
		public TransactionInterceptor transactionInterceptor(TransactionAttributeSource transactionAttributeSource,
				CouchbaseTransactionManager txManager) {
			TransactionInterceptor interceptor = new CouchbaseTransactionInterceptor();
			interceptor.setTransactionAttributeSource(transactionAttributeSource);
			if (txManager != null) {
				interceptor.setTransactionManager(txManager);
			}
			return interceptor;
		}

		@Bean
		@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
		public TransactionAttributeSource transactionAttributeSource() {
			return new AnnotationTransactionAttributeSource();
		}

		@Bean(name = TransactionManagementConfigUtils.TRANSACTION_ADVISOR_BEAN_NAME)
		@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
		public BeanFactoryTransactionAttributeSourceAdvisor transactionAdvisor(
				TransactionAttributeSource transactionAttributeSource, TransactionInterceptor transactionInterceptor) {

			BeanFactoryTransactionAttributeSourceAdvisor advisor = new BeanFactoryTransactionAttributeSourceAdvisor();
			advisor.setTransactionAttributeSource(transactionAttributeSource);
			advisor.setAdvice(transactionInterceptor);
			// if (this.enableTx != null) {
			// advisor.setOrder(this.enableTx.<Integer>getNumber("order"));
			// }
			return advisor;
		}

	}

	@Service
	@Component
	@EnableTransactionManagement
	static
	// @Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	class PersonService {

		final CouchbaseOperations personOperations;
		final CouchbaseCallbackTransactionManager manager; // final ReactiveCouchbaseTransactionManager manager;
		final ReactiveCouchbaseOperations personOperationsRx;
		final ReactiveCouchbaseTransactionManager managerRx;

		public PersonService(CouchbaseOperations ops, 	CouchbaseCallbackTransactionManager mgr, ReactiveCouchbaseOperations opsRx,
				ReactiveCouchbaseTransactionManager mgrRx) {
			personOperations = ops;
			manager = mgr;
			System.err.println("operations cluster  : " + personOperations.getCouchbaseClientFactory().getCluster());
//			System.err.println("manager cluster     : " + manager.getDatabaseFactory().getCluster());
			System.err.println("manager Manager     : " + manager);

			personOperationsRx = opsRx;
			managerRx = mgrRx;
			System.out
					.println("operationsRx cluster  : " + personOperationsRx.getCouchbaseClientFactory().getCluster().block());
			System.out.println("managerRx cluster     : " + mgrRx.getDatabaseFactory().getCluster().block());
			System.out.println("managerRx Manager     : " + managerRx);
			return;
		}

		public Person savePersonErrors(Person person) {
			assertInAnnotationTransaction(false);
			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
					new DefaultTransactionDefinition());

			return personOperationsRx.insertById(Person.class).one(person)//
					.<Person> flatMap(it -> Mono.error(new SimulateFailureException()))//
					.as(transactionalOperator::transactional).block();
		}

		public Person savePerson(Person person) {
			assertInAnnotationTransaction(false);
			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
					new DefaultTransactionDefinition());
			return personOperationsRx.insertById(Person.class).one(person)//
					.as(transactionalOperator::transactional).block();
		}

		public Long countDuringTx(Person person) {
			assertInAnnotationTransaction(false);
			assertInAnnotationTransaction(false);
			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
					new DefaultTransactionDefinition());

			return personOperationsRx.insertById(Person.class).one(person)//
					.then(personOperationsRx.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count())
					.as(transactionalOperator::transactional).block();
		}

		// @Transactional
		public List<EventLog> saveWithLogs(Person person) {
			assertInAnnotationTransaction(false);
			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
					new DefaultTransactionDefinition());

			return Flux
					.merge(personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "beforeConvert")), //
							personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "afterConvert")), //
							personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "beforeInsert")), //
							personOperationsRx.insertById(Person.class).one(person), //
							personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "afterInsert"))) //
					.thenMany(personOperationsRx.findByQuery(EventLog.class).all()) //
					.as(transactionalOperator::transactional).collectList().block();

		}

		public List<EventLog> saveWithErrorLogs(Person person) {
			assertInAnnotationTransaction(false);
			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
					new DefaultTransactionDefinition());

			return Flux
					.merge(personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "beforeConvert")), //
							personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "afterConvert")), //
							personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "beforeInsert")), //
							personOperationsRx.insertById(Person.class).one(person), //
							personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "afterInsert"))) //
					.thenMany(personOperationsRx.findByQuery(EventLog.class).all()) //
					.<EventLog> flatMap(it -> Mono.error(new SimulateFailureException())).as(transactionalOperator::transactional)
					.collectList().block();

		}

		// org.springframework.beans.factory.NoUniqueBeanDefinitionException:
		// No qualifying bean of type 'org.springframework.transaction.TransactionManager' available: expected single
		// matching bean but found 2: reactiveCouchbaseTransactionManager,couchbaseTransactionManager
		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
		public Person declarativeSavePerson(Person person) {
			assertInAnnotationTransaction(true);
			return personOperations.insertById(Person.class).one(person);
		}

		public Person savePersonBlocking(Person person) {
			if (1 == 1)
				throw new RuntimeException("not implemented");
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

		@Autowired CouchbaseCallbackTransactionManager callbackTm;

		/**
		 * to execute while ThreadReplaceloop() is running should force a retry
		 *
		 * @param person
		 * @return
		 */
		@Transactional(transactionManager = BeanNames.COUCHBASE_CALLBACK_TRANSACTION_MANAGER)
		public Person declarativeFindReplacePersonCallback(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			System.err.println("declarativeFindReplacePersonCallback try: " + tryCount.incrementAndGet());
			System.err.println("declarativeFindReplacePersonCallback cluster : "
					+ callbackTm.template().getCouchbaseClientFactory().getCluster().block());
			System.err.println("declarativeFindReplacePersonCallback resourceHolder : "
					+ org.springframework.transaction.support.TransactionSynchronizationManager
							.getResource(callbackTm.template().getCouchbaseClientFactory().getCluster().block()));
			Person p = personOperations.findById(Person.class).one(person.getId().toString());
			return personOperations.replaceById(Person.class).one(p);
		}

		/**
		 * to execute while ThreadReplaceloop() is running should force a retry
		 *
		 * @param person
		 * @return
		 */
		@Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)
		public Mono<Person> declarativeFindReplacePersonReactive(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			System.err.println("declarativeFindReplacePersonReactive try: " + tryCount.incrementAndGet());
			/*  NoTransactionInContextException
			TransactionSynchronizationManager.forCurrentTransaction().flatMap( sm -> {
				System.err.println("declarativeFindReplacePersonReactive reactive resourceHolder : "+sm.getResource(callbackTm.template().getCouchbaseClientFactory().getCluster().block()));
				return Mono.just(sm);
			}).block();
			*/
			return personOperationsRx.findById(Person.class).one(person.getId().toString())
					.flatMap(p -> personOperationsRx.replaceById(Person.class).one(p));
		}

		/**
		 * to execute while ThreadReplaceloop() is running should force a retry
		 *
		 * @param person
		 * @return
		 */
		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER) // doesn't retry
		public Person declarativeFindReplacePerson(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			System.err.println("declarativeFindReplacePerson try: " + tryCount.incrementAndGet());
			Person p = personOperations.findById(Person.class).one(person.getId().toString());
			return personOperations.replaceById(Person.class).one(p);
		}

		@Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER) // doesn't retry
		public Mono<Person> declarativeSavePersonReactive(Person person) {
			assertInAnnotationTransaction(true);
			return personOperationsRx.insertById(Person.class).one(person);
		}

		@Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)
		public Mono<Person> declarativeSavePersonErrorsReactive(Person person) {
			assertInAnnotationTransaction(true);
			Mono<Person> p = personOperationsRx.insertById(Person.class).one(person); //
			SimulateFailureException.throwEx();
			return p;
		}

		void assertInAnnotationTransaction(boolean inTransaction) {
			StackTraceElement[] stack = Thread.currentThread().getStackTrace();
			for (StackTraceElement ste : stack) {
				if (ste.getClassName().startsWith("org.springframework.transaction.interceptor")) {
					if (inTransaction) {
						return;
					}
				}
			}
			if (!inTransaction) {
				return;
			}
			throw new RuntimeException(
					"in transaction = " + (!inTransaction) + " but expected in annotation transaction = " + inTransaction);
		}

	}
}
