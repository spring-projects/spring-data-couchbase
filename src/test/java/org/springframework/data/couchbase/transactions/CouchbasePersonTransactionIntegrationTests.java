//// todo gpx commenting as doesn't compile
///*
// * Copyright 2012-2021 the original author or authors
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *        https://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.springframework.data.couchbase.transactions;
//
//import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertFalse;
//import static org.junit.jupiter.api.Assertions.assertNull;
//import static org.junit.jupiter.api.Assertions.assertThrows;
//import static org.junit.jupiter.api.Assertions.assertTrue;
//import static org.springframework.data.couchbase.util.Util.assertInAnnotationTransaction;
//
//import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
//import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
//import com.couchbase.client.java.transactions.config.TransactionOptions;
//import lombok.Data;
//import org.springframework.data.couchbase.domain.Config;
//import reactor.core.publisher.Flux;
//import reactor.core.publisher.Mono;
//
//import java.time.Duration;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.function.Function;
//
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.dao.DataRetrievalFailureException;
//import org.springframework.dao.DuplicateKeyException;
//import org.springframework.data.couchbase.CouchbaseClientFactory;
//import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
//import org.springframework.data.couchbase.config.BeanNames;
//import org.springframework.data.couchbase.core.CouchbaseOperations;
//import org.springframework.data.couchbase.core.CouchbaseTemplate;
//import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
//import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
//import org.springframework.data.couchbase.core.query.Query;
//import org.springframework.data.couchbase.core.query.QueryCriteria;
//import org.springframework.data.couchbase.domain.Person;
//import org.springframework.data.couchbase.domain.PersonRepository;
//import org.springframework.data.couchbase.domain.ReactivePersonRepository;
//import org.springframework.data.couchbase.transaction.ClientSessionOptions;
//import org.springframework.data.couchbase.transaction.CouchbaseSimpleCallbackTransactionManager;
//import org.springframework.data.couchbase.transaction.CouchbaseTransactionDefinition;
//import org.springframework.data.couchbase.transaction.ReactiveCouchbaseResourceHolder;
//import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
//import org.springframework.data.couchbase.transaction.TransactionsWrapper;
//import org.springframework.data.couchbase.util.Capabilities;
//import org.springframework.data.couchbase.util.ClusterType;
//import org.springframework.data.couchbase.util.IgnoreWhen;
//import org.springframework.data.couchbase.util.JavaIntegrationTests;
//import org.springframework.stereotype.Component;
//import org.springframework.stereotype.Service;
//import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
//import org.springframework.transaction.ReactiveTransaction;
//import org.springframework.transaction.TransactionDefinition;
//import org.springframework.transaction.annotation.EnableTransactionManagement;
//import org.springframework.transaction.annotation.Transactional;
//import org.springframework.transaction.reactive.TransactionContextManager;
//import org.springframework.transaction.reactive.TransactionSynchronizationManager;
//import org.springframework.transaction.reactive.TransactionalOperator;
//import org.springframework.transaction.support.DefaultTransactionDefinition;
//
//import com.couchbase.client.core.error.DocumentNotFoundException;
//import com.couchbase.client.core.error.transaction.RetryTransactionException;
//import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
//import com.couchbase.client.java.Collection;
//import com.couchbase.client.java.ReactiveCollection;
//import com.couchbase.client.java.kv.RemoveOptions;
//import com.couchbase.client.java.transactions.TransactionResult;
//import com.couchbase.client.java.transactions.Transactions;
//import com.couchbase.client.java.transactions.error.TransactionFailedException;
//
///**
// * Tests for com.couchbase.transactions using
// * <li><le>couchbase reactive transaction manager via transactional operator</le> <le>couchbase non-reactive transaction
// * manager via @Transactional</le> <le>@Transactional(transactionManager =
// * BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)</le></li>
// *
// * @author Michael Reiche
// */
//@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
//@SpringJUnitConfig(classes = { Config.class, CouchbasePersonTransactionIntegrationTests.PersonService.class })
//public class CouchbasePersonTransactionIntegrationTests extends JavaIntegrationTests {
//
//	@Autowired CouchbaseClientFactory couchbaseClientFactory;
//	@Autowired ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory;
//	@Autowired ReactiveCouchbaseTransactionManager reactiveCouchbaseTransactionManager;
//	@Autowired ReactivePersonRepository rxRepo;
//	@Autowired PersonRepository repo;
//	@Autowired CouchbaseTemplate cbTmpl;
//	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;
//	@Autowired PersonService personService;
//	@Autowired CouchbaseTemplate operations;
//
//	// if these are changed from default, then beforeEach needs to clean up separately
//	String sName = "_default";
//	String cName = "_default";
//
//	// static GenericApplicationContext context;
//
//	@BeforeAll
//	public static void beforeAll() {
//		callSuperBeforeAll(new Object() {});
//		// context = new AnnotationConfigApplicationContext(Config.class, PersonService.class);
//	}
//
//	@AfterAll
//	public static void afterAll() {
//		callSuperAfterAll(new Object() {});
//		// if (context != null) {
//		// context.close();
//		// }
//	}
//
//	@BeforeEach
//	public void beforeEachTest() {
//		// personService = context.getBean(PersonService.class); // getting it via autowired results in no @Transactional
//		// Skip this as we just one to track TransactionContext
//		operations.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all(); // doesn't work???
//		operations.removeByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all();
//		List<Person> p = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
//		List<EventLog> e = operations.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all();
//
//		Person walterWhite = new Person(1, "Walter", "White");
//		try {
//			couchbaseClientFactory.getBucket().defaultCollection().remove(walterWhite.getId().toString());
//		} catch (Exception ex) {
//			// System.err.println(ex);
//		}
//	}
//
//	@Test
//	public void shouldRollbackAfterException() {
//		Person p = new Person(null, "Walter", "White");
//		assertThrows(SimulateFailureException.class, () -> personService.savePersonErrors(p));
//		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
//		assertEquals(0, count, "should have done roll back and left 0 entries");
//	}
//
//	@Test
//	public void shouldRollbackAfterExceptionOfTxAnnotatedMethod() {
//		Person p = new Person(null, "Walter", "White");
//		assertThrowsOneOf(() -> personService.declarativeSavePersonErrors(p), TransactionFailedException.class);
//		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
//		assertEquals(0, count, "should have done roll back and left 0 entries");
//	}
//
//	@Test
//	public void shouldRollbackAfterExceptionOfTxAnnotatedMethodReactive() {
//		Person p = new Person(null, "Walter", "White");
//		assertThrows(SimulateFailureException.class, () -> personService.declarativeSavePersonErrorsReactive(p).block());
//		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
//		assertEquals(0, count, "should have done roll back and left 0 entries");
//	}
//
//	@Test
//	public void commitShouldPersistTxEntries() {
//		Person p = new Person(null, "Walter", "White");
//		Person s = personService.savePerson(p);
//		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
//		assertEquals(1, count, "should have saved and found 1");
//	}
//
//	@Test
//	public void commitShouldPersistTxEntriesOfTxAnnotatedMethod() {
//		Person p = new Person(null, "Walter", "White");
//		Person s = personService.declarativeSavePerson(p);
//		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
//		assertEquals(1, count, "should have saved and found 1");
//	}
//
//	@Test
//	public void replaceInTxAnnotatedCallback() {
//		Person person = new Person(1, "Walter", "White");
//		Person switchedPerson = new Person(1, "Dave", "Reynolds");
//		cbTmpl.insertById(Person.class).one(person);
//		AtomicInteger tryCount = new AtomicInteger(0);
//		Person p = personService.declarativeFindReplacePersonCallback(switchedPerson, tryCount);
//		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
//		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
//	}
//
//	@Test
//	public void commitShouldPersistTxEntriesOfTxAnnotatedMethodReactive() {
//		Person p = new Person(null, "Walter", "White");
//		Person s = personService.declarativeSavePersonReactive(p).block();
//		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
//		assertEquals(1, count, "should have saved and found 1");
//	}
//
//	@Test
//	public void commitShouldPersistTxEntriesAcrossCollections() {
//		List<EventLog> persons = personService.saveWithLogs(new Person(null, "Walter", "White"));
//		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
//		assertEquals(1, count, "should have saved and found 1");
//		Long countEvents = operations.count(new Query(), EventLog.class); //
//		assertEquals(4, countEvents, "should have saved and found 4");
//	}
//
//	@Test
//	public void rollbackShouldAbortAcrossCollections() {
//		assertThrows(SimulateFailureException.class,
//				() -> personService.saveWithErrorLogs(new Person(null, "Walter", "White")));
//		List<Person> persons = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
//		assertEquals(0, persons.size(), "should have done roll back and left 0 entries");
//		List<EventLog> events = operations.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all(); //
//		assertEquals(0, events.size(), "should have done roll back and left 0 entries");
//	}
//
//	@Test
//	public void countShouldWorkInsideTransaction() {
//		Long count = personService.countDuringTx(new Person(null, "Walter", "White"));
//		assertEquals(1, count, "should have counted 1 during tx");
//	}
//
//	@Test
//	public void emitMultipleElementsDuringTransaction() {
//		List<EventLog> docs = personService.saveWithLogs(new Person(null, "Walter", "White"));
//		assertEquals(4, docs.size(), "should have found 4 eventlogs");
//	}
//
//	@Test
//	public void errorAfterTxShouldNotAffectPreviousStep() {
//		Person p = personService.savePerson(new Person(null, "Walter", "White"));
//		// todo gp user shouldn't be getting exposed to TransactionOperationFailedException
//		assertThrows(TransactionOperationFailedException.class, () -> personService.savePerson(p));
//		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
//		assertEquals(1, count, "should have saved and found 1");
//	}
//
//	/**
//	 * This will appear to work even if replaceById does not use a transaction.
//	 */
//	@Test
//	@Disabled
//	public void replacePersonCBTransactionsRxTmpl() {
//		Person person = new Person(1, "Walter", "White");
//		cbTmpl.insertById(Person.class).one(person);
//		Mono<TransactionResult> result = this.couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> {
//			ReactiveCouchbaseResourceHolder resourceHolder = new ReactiveCouchbaseResourceHolder(AttemptContextReactiveAccessor.getCore(ctx));
//			Mono<TransactionSynchronizationManager> sync = TransactionContextManager.currentContext()
//					.map(TransactionSynchronizationManager::new).flatMap(synchronizationManager -> {
//						synchronizationManager.bindResource(reactiveCouchbaseClientFactory.getCluster().block(), resourceHolder);
//						prepareSynchronization(synchronizationManager, null, new CouchbaseTransactionDefinition());
//						return rxCBTmpl.findById(Person.class).one(person.getId().toString()) //
//								.flatMap((pp) -> rxCBTmpl.replaceById(Person.class).one(pp)) //
//								.then(Mono.just(synchronizationManager)); // tx
//					});
//			return sync.contextWrite(TransactionContextManager.getOrCreateContext())
//					.contextWrite(TransactionContextManager.getOrCreateContextHolder()).then();
//		});
//
//		result.block();
//		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
//		assertEquals(person, pFound, "should have found expected");
//	}
//
//	@Test
//	public void insertPersonCBTransactionsRxTmplRollback() {
//		Person person = new Person(1, "Walter", "White");
//		try {
//			rxCBTmpl.removeById(Person.class).one(person.getId().toString());
//		} catch (DocumentNotFoundException dnfe) {}
//		Mono<TransactionResult> result = couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> {
//			ReactiveCouchbaseResourceHolder resourceHolder = new ReactiveCouchbaseResourceHolder(AttemptContextReactiveAccessor.getCore(ctx));
//			Mono<TransactionSynchronizationManager> sync = TransactionContextManager.currentContext()
//					.map(TransactionSynchronizationManager::new).flatMap(synchronizationManager -> {
//						synchronizationManager.bindResource(reactiveCouchbaseClientFactory.getCluster().block(), resourceHolder);
//						prepareSynchronization(synchronizationManager, null, new CouchbaseTransactionDefinition());
//						// execute the transaction (insertById, SimulateFailure), insertById() will fetch the ctx from the context
//						return rxCBTmpl.insertById(Person.class).one(person).then(Mono.error(new SimulateFailureException())); // tx
//					});
//			return sync.contextWrite(TransactionContextManager.getOrCreateContext())
//					.contextWrite(TransactionContextManager.getOrCreateContextHolder()).then();
//
//		});
//		assertThrowsCause(TransactionFailedException.class, SimulateFailureException.class, (ignore) -> {
//			result.block();
//			return null;
//		});
//		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
//		assertNull(pFound, "insert should have been rolled back");
//	}
//
//	@Test
//	public void insertTwicePersonCBTransactionsRxTmplRollback() {
//		Person person = new Person(1, "Walter", "White");
//		//
//		Transactions transactions = this.couchbaseClientFactory.getCluster().transactions();
//		Mono<TransactionResult> result = couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> {
//
//			ReactiveCouchbaseResourceHolder resourceHolder = new ReactiveCouchbaseResourceHolder(AttemptContextReactiveAccessor.getCore(ctx));
//			Mono<TransactionSynchronizationManager> sync = TransactionContextManager.currentContext()
//					.map(TransactionSynchronizationManager::new).flatMap(synchronizationManager -> {
//						synchronizationManager.bindResource(reactiveCouchbaseClientFactory.getCluster().block(), resourceHolder);
//						prepareSynchronization(synchronizationManager, null, new CouchbaseTransactionDefinition());
//						return rxCBTmpl.insertById(Person.class).one(person) //
//								.flatMap((ppp) -> rxCBTmpl.insertById(Person.class).one(ppp)) //
//								.then(Mono.just(synchronizationManager)); // tx
//					});
//			return sync.contextWrite(TransactionContextManager.getOrCreateContext())
//					.contextWrite(TransactionContextManager.getOrCreateContextHolder()).then();
//		});
//		assertThrowsCause(TransactionFailedException.class, DuplicateKeyException.class, (ignore) -> {
//			result.block();
//			return null;
//		});
//		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
//		assertNull(pFound, "insert should have been rolled back");
//	}
//
//	/**
//	 * This test has the bare minimum for reactive transactions. Create the ClientSession that holds the ctx and put it in
//	 * a resourceHolder and binds it to the currentContext. The retries are handled by couchbase-transactions - which
//	 * creates a new ctx and re-runs the lambda. This is essentially what TransactionWrapper does.
//	 */
//	@Test
//	public void replaceWithCasConflictResolvedViaRetry() {
//		Person person = new Person(1, "Walter", "White");
//		Person switchedPerson = new Person(1, "Dave", "Reynolds");
//		AtomicBoolean stop = new AtomicBoolean(false);
//		Thread t = new ReplaceLoopThread(stop, switchedPerson);
//		t.start();
//		cbTmpl.insertById(Person.class).one(person);
//
//		AtomicInteger tryCount = new AtomicInteger(0);
//		Mono<TransactionResult> result = couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> {
//			ReactiveCouchbaseResourceHolder resourceHolder = new ReactiveCouchbaseResourceHolder(AttemptContextReactiveAccessor.getCore(ctx));
//			Mono<TransactionSynchronizationManager> sync = TransactionContextManager.currentContext()
//					.map(TransactionSynchronizationManager::new).flatMap(synchronizationManager -> {
//						synchronizationManager.bindResource(reactiveCouchbaseClientFactory.getCluster().block(), resourceHolder);
//						prepareSynchronization(synchronizationManager, null, new CouchbaseTransactionDefinition());
//						return rxCBTmpl.findById(Person.class).one(person.getId().toString()) //
//								.flatMap((ppp) -> {
//									tryCount.getAndIncrement();
//									System.err.println("===== ATTEMPT : " + tryCount.get() + " =====");
//									return Mono.just(ppp);
//								})//
//								.flatMap((ppp) -> rxCBTmpl.replaceById(Person.class).one(ppp)) //
//								.then(Mono.just(synchronizationManager)); // tx
//					});
//			return sync.contextWrite(TransactionContextManager.getOrCreateContext())
//					.contextWrite(TransactionContextManager.getOrCreateContextHolder()).then();
//		});
//
//		result.block();
//
//		stop.set(true);
//		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
//		assertTrue(tryCount.get() > 1, "should have been more than one try ");
//		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
//	}
//
//	@Test
//	public void wrapperReplaceWithCasConflictResolvedViaRetry() {
//		Person person = new Person(1, "Walter", "White");
//		Person switchedPerson = new Person(1, "Dave", "Reynolds");
//		AtomicInteger tryCount = new AtomicInteger(0);
//		cbTmpl.insertById(Person.class).one(person);
//
//		for (int i = 0; i < 10; i++) { // the transaction sometimes succeeds on the first try
//			AtomicBoolean stop = new AtomicBoolean(false);
//			Thread t = new ReplaceLoopThread(stop, switchedPerson);
//			t.start();
//			tryCount.set(0);
//			TransactionsWrapper transactionsWrapper = new TransactionsWrapper(reactiveCouchbaseClientFactory);
//			Mono<TransactionResult> result = transactionsWrapper.reactive(ctx -> {
//				System.err.println("try: " + tryCount.incrementAndGet());
//				return rxCBTmpl.findById(Person.class).one(person.getId().toString()) //
//						.flatMap((ppp) -> rxCBTmpl.replaceById(Person.class).one(ppp)).then();
//			});
//			TransactionResult txResult = result.block();
//			stop.set(true);
//			System.out.println("txResult: " + txResult);
//			if (tryCount.get() > 1) {
//				break;
//			}
//		}
//		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
//		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
//		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
//	}
//
//	/**
//	 * This does process retries - by CallbackTransactionManager.execute() -> transactions.run() -> executeTransaction()
//	 * -> retryWhen. The CallbackTransactionManager only finds the resources in the Thread - it doesn't find it in the
//	 * context. It might be nice to use the context for both - but I'm not sure if that is possible - mostly due to
//	 * ExecutableFindById.one() calling reactive.one().block() instead of returning a publisher which could have
//	 * .contextWrite() called on it.
//	 */
//	@Test
//	public void replaceWithCasConflictResolvedViaRetryAnnotatedCallback() {
//		Person person = new Person(1, "Walter", "White");
//		Person switchedPerson = new Person(1, "Dave", "Reynolds");
//		AtomicInteger tryCount = new AtomicInteger(0);
//		cbTmpl.insertById(Person.class).one(person);
//
//		for (int i = 0; i < 10; i++) { // the transaction sometimes succeeds on the first try
//			AtomicBoolean stop = new AtomicBoolean(false);
//			Thread t = new ReplaceLoopThread(stop, switchedPerson);
//			t.start();
//			tryCount.set(0);
//
//			Person p = personService.declarativeFindReplacePersonCallback(switchedPerson, tryCount);
//			stop.set(true);
//			if (tryCount.get() > 1) {
//				break;
//			}
//		}
//		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
//		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
//		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
//
//	}
//
//	/**
//	 * Reactive @Transactional does not retry write-write conflicts. It throws RetryTransactionException up to the client
//	 * and expects the client to retry.
//	 */
//	@Test
//	public void replaceWithCasConflictResolvedViaRetryAnnotatedReactive() {
//		Person person = new Person(1, "Walter", "White");
//		Person switchedPerson = new Person(1, "Dave", "Reynolds");
//		cbTmpl.insertById(Person.class).one(person);
//		AtomicInteger tryCount = new AtomicInteger(0);
//		for (int i = 0; i < 10; i++) { // the transaction sometimes succeeds on the first try
//			AtomicBoolean stop = new AtomicBoolean(false);
//			Thread t = new ReplaceLoopThread(stop, switchedPerson);
//			t.start();
//			tryCount.set(0);
//			for (;;) {
//				Person res = personService.declarativeFindReplacePersonReactive(switchedPerson, tryCount)
//						.onErrorResume(RetryTransactionException.class, (thrown) -> Mono.empty()).block();
//				if (res != null)
//					break;
//			}
//			stop.set(true);
//			if (tryCount.get() > 1) {
//				break;
//			}
//
//		}
//		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
//		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
//		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
//	}
//
//	@Test
//	/**
//	 * This fails with TransactionOperationFailed {ec:FAIL_CAS_MISMATCH, retry:true, autoRollback:true}. I don't know why
//	 * it isn't retried. This seems like it is due to the functioning of AbstractPlatformTransactionManager
//	 */
//	public void replaceWithCasConflictResolvedViaRetryAnnotated() {
//		Person person = new Person(1, "Walter", "White");
//		Person switchedPerson = new Person(1, "Dave", "Reynolds");
//		cbTmpl.insertById(Person.class).one(person);
//		AtomicInteger tryCount = new AtomicInteger(0);
//
//		for (int i = 0; i < 10; i++) { // the transaction sometimes succeeds on the first try
//			AtomicBoolean stop = new AtomicBoolean(false);
//			Thread t = new ReplaceLoopThread(stop, switchedPerson);
//			t.start();
//			tryCount.set(0);
//			Person p = personService.declarativeFindReplacePerson(person, tryCount);
//			stop.set(true);
//			if (tryCount.get() > 1) {
//				break;
//			}
//		}
//		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
//		System.out.println("pFound: " + pFound);
//		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
//		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
//	}
//
//	private class ReplaceLoopThread extends Thread {
//		AtomicBoolean stop;
//		Person person;
//		int maxIterations = 100;
//
//		public ReplaceLoopThread(AtomicBoolean stop, Person person, int... iterations) {
//			this.stop = stop;
//			this.person = person;
//			if (iterations != null && iterations.length == 1) {
//				this.maxIterations = iterations[0];
//			}
//		}
//
//		public void run() {
//			for (int i = 0; i < maxIterations && !stop.get(); i++) {
//				sleepMs(10);
//				try {
//					// note that this does not go through spring-data, therefore it does not have the @Field , @Version etc.
//					// annotations processed so we just check getFirstname().equals()
//					// switchedPerson has version=0, so it doesn't check CAS
//					couchbaseClientFactory.getBucket().defaultCollection().replace(person.getId().toString(), person);
//					System.out.println("********** replace thread: " + i + " success");
//				} catch (Exception e) {
//					System.out.println("********** replace thread: " + i + " " + e.getClass().getName());
//				}
//			}
//
//		}
//	}
//
//	@Test
//	public void replacePersonCBTransactionsRxTmplRollback() {
//		Person person = new Person(1, "Walter", "White");
//		String newName = "Walt";
//		rxCBTmpl.insertById(Person.class).one(person).block();
//
//		Mono<TransactionResult> result = reactiveCouchbaseClientFactory.getCluster().block().reactive().transactions()
//				.run(ctx -> {
//					// can we take the ReactiveTransactionAttemptContext ctx and save it in the context?
//					// how we get from non-reactive to reactive?
//					ReactiveCouchbaseResourceHolder resourceHolder = new ReactiveCouchbaseResourceHolder(AttemptContextReactiveAccessor.getCore(ctx));
//
//					assertEquals(reactiveCouchbaseClientFactory.getCluster().block(), couchbaseClientFactory.getCluster());
//					// I think this needs to happen within the couchbaseClientFactory.getCluster().reactive().transactions().run()
//					// call - or equivalent.
//					// this currentContext() call is going to create a new ctx, and store the acr. Will it get used in
//					// syncFlatMap()
//					// below? Should the ctx be created in the above call to
//					// couchbaseClientFactory.getCluster().reactive().transactions().run()?
//					// How does this work in savePerson etc?
//					// is there means for just getting the currentContext() without creating it?
//					return TransactionContextManager.currentContext().map(TransactionSynchronizationManager::new)
//							.flatMap(synchronizationManager -> {
//								// is this the correct sync Manager??
//								synchronizationManager.bindResource(reactiveCouchbaseClientFactory.getCluster().block(),
//										resourceHolder);
//								prepareSynchronization(synchronizationManager, null, new CouchbaseTransactionDefinition());
//								return rxCBTmpl.findById(Person.class).one(person.getId().toString());
//							}) // need to get the TSM context in the one() calls.
//							.flatMap(pp -> rxCBTmpl.replaceById(Person.class).one(pp.withFirstName(newName))).then(Mono.empty());
//				}).contextWrite(TransactionContextManager.getOrCreateContext())
//				.contextWrite(TransactionContextManager.getOrCreateContextHolder());
//
//		result.block();
//		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
//		System.err.println(pFound);
//		assertEquals(newName, pFound.getFirstname());
//	}
//
//	@Test
//	public void deletePersonCBTransactionsRxTmpl() {
//		Person person = new Person(1, "Walter", "White");
//		remove(cbTmpl, cName, person.getId().toString());
//		rxCBTmpl.insertById(Person.class).inCollection(cName).one(person).block();
//
//		TransactionsWrapper transactionsWrapper = new TransactionsWrapper(reactiveCouchbaseClientFactory);
//		Mono<TransactionResult> result = transactionsWrapper.run(ctx -> { // get the ctx
//			return rxCBTmpl.removeById(Person.class).inCollection(cName).oneEntity(person).then();
//		});
//		result.block();
//		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
//		assertNull(pFound, "Should not have found " + pFound);
//	}
//
//	@Test
//	public void deletePersonCBTransactionsRxTmplFail() {
//		Person person = new Person(1, "Walter", "White");
//		remove(cbTmpl, cName, person.getId().toString());
//		cbTmpl.insertById(Person.class).inCollection(cName).one(person);
//
//		TransactionsWrapper transactionsWrapper = new TransactionsWrapper(reactiveCouchbaseClientFactory);
//		Mono<TransactionResult> result = transactionsWrapper.run(ctx -> { // get the ctx
//			return rxCBTmpl.removeById(Person.class).inCollection(cName).oneEntity(person)
//					.then(rxCBTmpl.removeById(Person.class).inCollection(cName).oneEntity(person)).then();
//		});
//		assertThrowsWithCause(result::block, TransactionFailedException.class, DataRetrievalFailureException.class);
//		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
//		assertEquals(pFound, person, "Should have found " + person);
//	}
//
//	// RxRepo ////////////////////////////////////////////////////////////////////////////////////////////
//
//	@Test
//	public void deletePersonCBTransactionsRxRepo() {
//		Person person = new Person(1, "Walter", "White");
//		remove(cbTmpl, cName, person.getId().toString());
//		rxRepo.withCollection(cName).save(person).block();
//
//		TransactionsWrapper transactionsWrapper = new TransactionsWrapper(reactiveCouchbaseClientFactory);
//		Mono<TransactionResult> result = transactionsWrapper.run(ctx -> { // get the ctx
//			return rxRepo.withCollection(cName).delete(person).then();
//		});
//		result.block();
//		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
//		assertNull(pFound, "Should not have found " + pFound);
//	}
//
//	@Test
//	public void deletePersonCBTransactionsRxRepoFail() {
//		Person person = new Person(1, "Walter", "White");
//		remove(cbTmpl, cName, person.getId().toString());
//		rxRepo.withCollection(cName).save(person).block();
//
//		TransactionsWrapper transactionsWrapper = new TransactionsWrapper(reactiveCouchbaseClientFactory);
//		Mono<TransactionResult> result = transactionsWrapper.run(ctx -> { // get the ctx
//			return rxRepo.withCollection(cName).delete(person).then(rxRepo.withCollection(cName).delete(person)).then();
//		});
//		assertThrowsWithCause(result::block, TransactionFailedException.class, DataRetrievalFailureException.class);
//		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
//		assertEquals(pFound, person, "Should have found " + person);
//	}
//
//	@Test
//	public void findPersonCBTransactions() {
//		Person person = new Person(1, "Walter", "White");
//		remove(cbTmpl, cName, person.getId().toString());
//		cbTmpl.insertById(Person.class).inScope(sName).inCollection(cName).one(person);
//		List<Object> docs = new LinkedList<>();
//		Query q = Query.query(QueryCriteria.where("meta().id").eq(person.getId()));
//		TransactionsWrapper transactionsWrapper = new TransactionsWrapper(reactiveCouchbaseClientFactory);
//		Mono<TransactionResult> result = transactionsWrapper.run(ctx -> rxCBTmpl.findByQuery(Person.class).inScope(sName)
//				.inCollection(cName).matching(q).withConsistency(REQUEST_PLUS).one().doOnSuccess(doc -> docs.add(doc)));
//		result.block();
//		assertFalse(docs.isEmpty(), "Should have found " + person);
//		for (Object o : docs) {
//			assertEquals(o, person, "Should have found " + person);
//		}
//	}
//
//	@Test
//	public void insertPersonRbCBTransactions() {
//		Person person = new Person(1, "Walter", "White");
//		remove(cbTmpl, cName, person.getId().toString());
//
//		TransactionsWrapper transactionsWrapper = new TransactionsWrapper(reactiveCouchbaseClientFactory);
//		Mono<TransactionResult> result = transactionsWrapper.run(ctx -> rxCBTmpl.insertById(Person.class).inScope(sName)
//				.inCollection(cName).one(person).<Person> flatMap(it -> Mono.error(new SimulateFailureException())));
//		try {
//			result.block();
//		} catch (TransactionFailedException e) {
//			if (e.getCause() instanceof SimulateFailureException) {
//				Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
//				assertNull(pFound, "Should not have found " + pFound);
//				return;
//			} else {
//				e.printStackTrace();
//			}
//		}
//		throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
//	}
//
//	@Test
//	public void replacePersonRbCBTransactions() {
//		Person person = new Person(1, "Walter", "White");
//		remove(cbTmpl, cName, person.getId().toString());
//		cbTmpl.insertById(Person.class).inScope(sName).inCollection(cName).one(person);
//		TransactionsWrapper transactionsWrapper = new TransactionsWrapper(reactiveCouchbaseClientFactory);
//		Mono<TransactionResult> result = transactionsWrapper.run(ctx -> rxCBTmpl.findById(Person.class).inScope(sName)
//				.inCollection(cName).one(person.getId().toString()).flatMap(pFound -> rxCBTmpl.replaceById(Person.class)
//						.inScope(sName).inCollection(cName).one(pFound.withFirstName("Walt")))
//				.<Person> flatMap(it -> Mono.error(new SimulateFailureException())));
//		try {
//			result.block();
//		} catch (TransactionFailedException e) {
//			if (e.getCause() instanceof SimulateFailureException) {
//				Person pFound = cbTmpl.findById(Person.class).inScope(sName).inCollection(cName).one(person.getId().toString());
//				assertEquals(person, pFound, "Should have found " + person);
//				return;
//			} else {
//				e.printStackTrace();
//			}
//		}
//		throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
//	}
//
//	@Test
//	public void findPersonSpringTransactions() {
//		Person person = new Person(1, "Walter", "White");
//		remove(cbTmpl, cName, person.getId().toString());
//		cbTmpl.insertById(Person.class).inScope(sName).inCollection(cName).one(person);
//		// sleepMs(1000);
//		List<Object> docs = new LinkedList<>();
//		Query q = Query.query(QueryCriteria.where("meta().id").eq(person.getId()));
//		TransactionsWrapper transactionsWrapper = new TransactionsWrapper(reactiveCouchbaseClientFactory);
//		Mono<TransactionResult> result = transactionsWrapper.run(ctx -> rxCBTmpl.findByQuery(Person.class).inScope(sName)
//				.inCollection(cName).matching(q).one().doOnSuccess(r -> docs.add(r)));
//		result.block();
//		assertFalse(docs.isEmpty(), "Should have found " + person);
//		for (Object o : docs) {
//			assertEquals(o, person, "Should have found " + person);
//		}
//	}
//
//	void remove(Collection col, String id) {
//		remove(col.reactive(), id);
//	}
//
//	void remove(ReactiveCollection col, String id) {
//		try {
//			col.remove(id, RemoveOptions.removeOptions().timeout(Duration.ofSeconds(10))).block();
//		} catch (DocumentNotFoundException nfe) {
//			System.out.println(id + " : " + "DocumentNotFound when deleting");
//		}
//	}
//
//	void remove(CouchbaseTemplate template, String collection, String id) {
//		remove(template.reactive(), collection, id);
//	}
//
//	void remove(ReactiveCouchbaseTemplate template, String collection, String id) {
//		try {
//			template.removeById(Person.class).inCollection(collection).one(id).block();
//			System.out.println("removed " + id);
//		} catch (DocumentNotFoundException | DataRetrievalFailureException nfe) {
//			System.out.println(id + " : " + "DocumentNotFound when deleting");
//		}
//	}
//
//	private static void prepareSynchronization(TransactionSynchronizationManager synchronizationManager,
//											   ReactiveTransaction status, TransactionDefinition definition) {
//
//		// if (status.isNewSynchronization()) {
//		synchronizationManager.setActualTransactionActive(false /*status.hasTransaction()*/);
//		synchronizationManager.setCurrentTransactionIsolationLevel(
//				definition.getIsolationLevel() != TransactionDefinition.ISOLATION_DEFAULT ? definition.getIsolationLevel()
//						: null);
//		synchronizationManager.setCurrentTransactionReadOnly(definition.isReadOnly());
//		synchronizationManager.setCurrentTransactionName(definition.getName());
//		synchronizationManager.initSynchronization();
//		// }
//	}
//
//	void assertThrowsCause(Class<?> exceptionClass, Class<?> causeClass, Function<?, ?> function) {
//		try {
//			function.apply(null);
//		} catch (Throwable tfe) {
//			System.err.println("Exception: " + tfe + " causedBy: " + tfe.getCause());
//			if (tfe.getClass().isAssignableFrom(exceptionClass)) {
//				if (tfe.getCause() != null && tfe.getCause().getClass().isAssignableFrom(causeClass)) {
//					System.err.println("thrown exception was: " + tfe + " cause: " + tfe.getCause());
//					return;
//				}
//			}
//			throw new RuntimeException("expected " + exceptionClass + " with cause " + causeClass + " but got " + tfe);
//		}
//		throw new RuntimeException("expected " + exceptionClass + " with cause " + causeClass + " nothing was thrown");
//	}
//
//	@Data
//	// @AllArgsConstructor
//	static class EventLog {
//		public EventLog() {}
//
//		public EventLog(ObjectId oid, String action) {
//			this.id = oid.toString();
//			this.action = action;
//		}
//
//		public EventLog(String id, String action) {
//			this.id = id;
//			this.action = action;
//		}
//
//		String id;
//		String action;
//
//		public String toString() {
//			StringBuilder sb = new StringBuilder();
//			sb.append("EventLog : {\n");
//			sb.append("  id : " + getId());
//			sb.append(", action: " + action);
//			return sb.toString();
//		}
//	}
//
//	// todo gp disabled while trying to get alternative method of CouchbaseCallbackTransactionManager working
//	// @Configuration(proxyBeanMethods = false)
//	// @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
//	// static class TransactionInterception {
//	//
//	// @Bean
//	// @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
//	// public TransactionInterceptor transactionInterceptor(TransactionAttributeSource transactionAttributeSource,
//	// CouchbaseTransactionManager txManager) {
//	// TransactionInterceptor interceptor = new CouchbaseTransactionInterceptor();
//	// interceptor.setTransactionAttributeSource(transactionAttributeSource);
//	// if (txManager != null) {
//	// interceptor.setTransactionManager(txManager);
//	// }
//	// return interceptor;
//	// }
//	//
//	// @Bean
//	// @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
//	// public TransactionAttributeSource transactionAttributeSource() {
//	// return new AnnotationTransactionAttributeSource();
//	// }
//	//
//	// @Bean(name = TransactionManagementConfigUtils.TRANSACTION_ADVISOR_BEAN_NAME)
//	// @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
//	// public BeanFactoryTransactionAttributeSourceAdvisor transactionAdvisor(
//	// TransactionAttributeSource transactionAttributeSource, TransactionInterceptor transactionInterceptor) {
//	//
//	// BeanFactoryTransactionAttributeSourceAdvisor advisor = new BeanFactoryTransactionAttributeSourceAdvisor();
//	// advisor.setTransactionAttributeSource(transactionAttributeSource);
//	// advisor.setAdvice(transactionInterceptor);
//	// // if (this.enableTx != null) {
//	// // advisor.setOrder(this.enableTx.<Integer>getNumber("order"));
//	// // }
//	// return advisor;
//	// }
//	//
//	// }
//
//	@Service
//	@Component
//	@EnableTransactionManagement
//	static
//			// @Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
//	class PersonService {
//
//		final CouchbaseOperations personOperations;
//		final CouchbaseSimpleCallbackTransactionManager manager; // final ReactiveCouchbaseTransactionManager manager;
//		final ReactiveCouchbaseOperations personOperationsRx;
//		final ReactiveCouchbaseTransactionManager managerRx;
//
//		public PersonService(CouchbaseOperations ops, CouchbaseSimpleCallbackTransactionManager mgr,
//							 ReactiveCouchbaseOperations opsRx, ReactiveCouchbaseTransactionManager mgrRx) {
//			personOperations = ops;
//			manager = mgr;
//			System.err.println("operations cluster  : " + personOperations.getCouchbaseClientFactory().getCluster());
//			// System.err.println("manager cluster : " + manager.getDatabaseFactory().getCluster());
//			System.err.println("manager Manager     : " + manager);
//
//			personOperationsRx = opsRx;
//			managerRx = mgrRx;
//			System.out
//					.println("operationsRx cluster  : " + personOperationsRx.getCouchbaseClientFactory().getCluster().block());
//			System.out.println("managerRx cluster     : " + mgrRx.getDatabaseFactory().getCluster().block());
//			System.out.println("managerRx Manager     : " + managerRx);
//			return;
//		}
//
//		public Person savePersonErrors(Person person) {
//			assertInAnnotationTransaction(false);
//			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
//					new DefaultTransactionDefinition());
//
//			return personOperationsRx.insertById(Person.class).one(person)//
//					.<Person> flatMap(it -> Mono.error(new SimulateFailureException()))//
//					.as(transactionalOperator::transactional).block();
//		}
//
//		public Person savePerson(Person person) {
//			assertInAnnotationTransaction(false);
//			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
//					new DefaultTransactionDefinition());
//			return personOperationsRx.insertById(Person.class).one(person)//
//					.as(transactionalOperator::transactional).block();
//		}
//
//		public Long countDuringTx(Person person) {
//			assertInAnnotationTransaction(false);
//			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
//					new DefaultTransactionDefinition());
//			return personOperationsRx.insertById(Person.class).one(person)//
//					.then(personOperationsRx.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count())
//					.as(transactionalOperator::transactional).block();
//		}
//
//		public List<EventLog> saveWithLogs(Person person) {
//			assertInAnnotationTransaction(false);
//			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
//					new DefaultTransactionDefinition());
//
//			return Flux
//					.merge(personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "beforeConvert")), //
//							personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "afterConvert")), //
//							personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "beforeInsert")), //
//							personOperationsRx.insertById(Person.class).one(person), //
//							personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "afterInsert"))) //
//					.thenMany(personOperationsRx.findByQuery(EventLog.class).all()) //
//					.as(transactionalOperator::transactional).collectList().block();
//
//		}
//
//		public List<EventLog> saveWithErrorLogs(Person person) {
//			assertInAnnotationTransaction(false);
//			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
//					new DefaultTransactionDefinition());
//
//			return Flux
//					.merge(personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "beforeConvert")), //
//							personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "afterConvert")), //
//							personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "beforeInsert")), //
//							personOperationsRx.insertById(Person.class).one(person), //
//							personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "afterInsert"))) //
//					.thenMany(personOperationsRx.findByQuery(EventLog.class).all()) //
//					.<EventLog> flatMap(it -> Mono.error(new SimulateFailureException())).as(transactionalOperator::transactional)
//					.collectList().block();
//
//		}
//
//		// org.springframework.beans.factory.NoUniqueBeanDefinitionException:
//		// No qualifying bean of type 'org.springframework.transaction.TransactionManager' available: expected single
//		// matching bean but found 2: reactiveCouchbaseTransactionManager,couchbaseTransactionManager
//		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
//		public Person declarativeSavePerson(Person person) {
//			assertInAnnotationTransaction(true);
//			return personOperations.insertById(Person.class).one(person);
//		}
//
//		public Person savePersonBlocking(Person person) {
//			if (1 == 1)
//				throw new RuntimeException("not implemented");
//			assertInAnnotationTransaction(true);
//			return personOperations.insertById(Person.class).one(person);
//
//		}
//
//		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
//		public Person declarativeSavePersonErrors(Person person) {
//			assertInAnnotationTransaction(true);
//			Person p = personOperations.insertById(Person.class).one(person); //
//			SimulateFailureException.throwEx();
//			return p;
//		}
//
//		@Autowired CouchbaseSimpleCallbackTransactionManager callbackTm;
//
//		/**
//		 * to execute while ThreadReplaceloop() is running should force a retry
//		 *
//		 * @param person
//		 * @return
//		 */
//		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
//		public Person declarativeFindReplacePersonCallback(Person person, AtomicInteger tryCount) {
//			assertInAnnotationTransaction(true);
//			System.err.println("declarativeFindReplacePersonCallback try: " + tryCount.incrementAndGet());
//			// System.err.println("declarativeFindReplacePersonCallback cluster : "
//			// + callbackTm.template().getCouchbaseClientFactory().getCluster().block());
//			// System.err.println("declarativeFindReplacePersonCallback resourceHolder : "
//			// + org.springframework.transaction.support.TransactionSynchronizationManager
//			// .getResource(callbackTm.template().getCouchbaseClientFactory().getCluster().block()));
//			Person p = personOperations.findById(Person.class).one(person.getId().toString());
//			return personOperations.replaceById(Person.class).one(p.withFirstName(person.getFirstname()));
//		}
//
//		/**
//		 * The ReactiveCouchbaseTransactionManager does not retry on write-write conflict. Instead it will throw
//		 * RetryTransactionException to execute while ThreadReplaceloop() is running should force a retry
//		 *
//		 * @param person
//		 * @return
//		 */
//		@Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)
//		public Mono<Person> declarativeFindReplacePersonReactive(Person person, AtomicInteger tryCount) {
//			assertInAnnotationTransaction(true);
//			System.err.println("declarativeFindReplacePersonReactive try: " + tryCount.incrementAndGet());
//			/*  NoTransactionInContextException
//			TransactionSynchronizationManager.forCurrentTransaction().flatMap( sm -> {
//				System.err.println("declarativeFindReplacePersonReactive reactive resourceHolder : "+sm.getResource(callbackTm.template().getCouchbaseClientFactory().getCluster().block()));
//				return Mono.just(sm);
//			}).block();
//			*/
//			return personOperationsRx.findById(Person.class).one(person.getId().toString())
//					.flatMap(p -> personOperationsRx.replaceById(Person.class).one(p.withFirstName(person.getFirstname())));
//		}
//
//		/**
//		 * to execute while ThreadReplaceloop() is running should force a retry
//		 *
//		 * @param person
//		 * @return
//		 */
//		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
//		public Person declarativeFindReplacePerson(Person person, AtomicInteger tryCount) {
//			assertInAnnotationTransaction(true);
//			System.err.println("declarativeFindReplacePerson try: " + tryCount.incrementAndGet());
//			Person p = personOperations.findById(Person.class).one(person.getId().toString());
//			return personOperations.replaceById(Person.class).one(p);
//		}
//
//		@Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER) // doesn't retry
//		public Mono<Person> declarativeSavePersonReactive(Person person) {
//			assertInAnnotationTransaction(true);
//			return personOperationsRx.insertById(Person.class).one(person);
//		}
//
//		@Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)
//		public Mono<Person> declarativeSavePersonErrorsReactive(Person person) {
//			assertInAnnotationTransaction(true);
//			Mono<Person> p = personOperationsRx.insertById(Person.class).one(person); //
//			SimulateFailureException.throwEx();
//			return p;
//		}
//
//	}
//
//}
