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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertInReactiveTransaction;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertInTransaction;
import static org.springframework.data.couchbase.util.Util.assertInAnnotationTransaction;

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import lombok.Data;
import org.junit.jupiter.api.AfterEach;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
import org.springframework.data.couchbase.transaction.CouchbaseSimpleCallbackTransactionManager;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionDefinition;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseResourceHolder;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
import org.springframework.data.couchbase.transaction.TransactionsWrapper;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionContextManager;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.transaction.RetryTransactionException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.Transactions;
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
@SpringJUnitConfig(classes = { CouchbasePersonTransactionIntegrationTests.Config.class, CouchbasePersonTransactionIntegrationTests.PersonService.class })
public class CouchbasePersonTransactionIntegrationTests extends JavaIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory;
	@Autowired ReactiveCouchbaseTransactionManager reactiveCouchbaseTransactionManager;
	@Autowired ReactivePersonRepository rxRepo;
	@Autowired PersonRepository repo;
	@Autowired CouchbaseTemplate cbTmpl;
	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;
	@Autowired PersonService personService;
	@Autowired CouchbaseTemplate operations;

	// if these are changed from default, then beforeEach needs to clean up separately
	String sName = "_default";
	String cName = "_default";
	private TransactionalOperator transactionalOperator;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
	}

	@AfterAll
	public static void afterAll() {
		callSuperAfterAll(new Object() {});
	}

	@AfterEach
	public void afterEachTest() {
		TransactionTestUtil.assertNotInTransaction();
	}

	@BeforeEach
	public void beforeEachTest() {
		TransactionTestUtil.assertNotInTransaction();
		List<RemoveResult> rp0 = operations.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<RemoveResult> rp1 = operations.removeByQuery(Person.class).inScope(sName).inCollection(cName)
				.withConsistency(REQUEST_PLUS).all();
		List<RemoveResult> rp2 = operations.removeByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all();
		List<RemoveResult> rp3 = operations.removeByQuery(EventLog.class).inScope(sName).inCollection(cName)
				.withConsistency(REQUEST_PLUS).all();

		List<Person> p0 = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<Person> p1 = operations.findByQuery(Person.class).inScope(sName).inCollection(cName)
				.withConsistency(REQUEST_PLUS).all();
		List<EventLog> e0 = operations.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all();
		List<EventLog> e1 = operations.findByQuery(EventLog.class).inScope(sName).inCollection(cName)
				.withConsistency(REQUEST_PLUS).all();

		Person walterWhite = new Person(1, "Walter", "White");
		remove(cbTmpl, sName, cName, walterWhite.getId().toString());
		transactionalOperator = TransactionalOperator.create(reactiveCouchbaseTransactionManager);
	}

	@Disabled("gp: as CouchbaseTransactionOperation or TransactionalOperator user")
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
		assertThrowsOneOf(() -> personService.declarativeSavePersonErrors(p), TransactionFailedException.class);
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
	public void replaceInTxAnnotatedCallback() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		cbTmpl.insertById(Person.class).one(person);
		AtomicInteger tryCount = new AtomicInteger(0);
		Person p = personService.declarativeFindReplacePersonCallback(switchedPerson, tryCount);
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
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

	@Disabled("gp: as CouchbaseTransactionOperation or TransactionalOperator user")
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

	@Disabled("gp: as CouchbaseTransactionOperation or TransactionalOperator user")
	@Test
	public void emitMultipleElementsDuringTransaction() {
		List<EventLog> docs = personService.saveWithLogs(new Person(null, "Walter", "White"));
		assertEquals(4, docs.size(), "should have found 4 eventlogs");
	}

	@Disabled("gp: as CouchbaseTransactionOperation or TransactionalOperator user")
	@Test
	public void errorAfterTxShouldNotAffectPreviousStep() {
		Person p = personService.savePerson(new Person(null, "Walter", "White"));
		// todo gp user shouldn't be getting exposed to TransactionOperationFailedException.  This is happening as TransactionOperator does not support the automatic retries and error handling we depend on.  As argued on Slack, we cannot support it because of this.
		// todo mr
		/*
		TransactionOperationFailedException {cause:com.couchbase.client.core.error.DocumentExistsException, retry:false, autoRollback:true, raise:TRANSACTION_FAILED}
		at com.couchbase.client.core.error.transaction.TransactionOperationFailedException$Builder.build(TransactionOperationFailedException.java:136)
		at com.couchbase.client.core.transaction.CoreTransactionAttemptContext.lambda$handleDocExistsDuringStagedInsert$116(CoreTransactionAttemptContext.java:1801)
		 */
		assertThrows(TransactionOperationFailedException.class, () -> personService.savePerson(p));
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	@Disabled
	public void replacePersonCBTransactionsRxTmpl() {
		Person person = new Person(1, "Walter", "White");
		cbTmpl.insertById(Person.class).one(person);
		Mono<Person> result = rxCBTmpl.findById(Person.class).one(person.getId().toString()) //
				.flatMap(pp -> rxCBTmpl.replaceById(Person.class).one(pp))
				.flatMap(ppp ->  assertInReactiveTransaction(ppp))
				.as(transactionalOperator::transactional);
		result.block();
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertEquals(person, pFound, "should have found expected " + person);
	}

	@Disabled("gp: as CouchbaseTransactionOperation or TransactionalOperator user")
	@Test
	public void insertPersonCBTransactionsRxTmplRollback() {
		Person person = new Person(1, "Walter", "White");
		Mono<Person> result = rxCBTmpl.insertById(Person.class).one(person) //
				.flatMap(ppp ->  assertInReactiveTransaction(ppp))
				.flatMap(p -> throwSimulatedFailure(p)).as(transactionalOperator::transactional); // tx
		assertThrows(SimulateFailureException.class, result::block);
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertNull(pFound, "insert should have been rolled back");
	}

	@Disabled("gp: as CouchbaseTransactionOperation or TransactionalOperator user")
	@Test
	public void insertTwicePersonCBTransactionsRxTmplRollback() {
		Person person = new Person(1, "Walter", "White");
		Mono<Person> result = rxCBTmpl.insertById(Person.class).one(person) //
				.flatMap(ppp -> rxCBTmpl.insertById(Person.class).one(ppp)) //
				.as(transactionalOperator::transactional);
		assertThrows(DuplicateKeyException.class, result::block);
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertNull(pFound, "insert should have been rolled back");
	}

	/**
	 * This test has the bare minimum for reactive transactions. Create the ClientSession that holds the ctx and put it in
	 * a resourceHolder and binds it to the currentContext. The retries are handled by couchbase-transactions - which
	 * creates a new ctx and re-runs the lambda. This is essentially what TransactionWrapper does.
	 */
	@Test
	public void wrapperReplaceWithCasConflictResolvedViaRetry() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		AtomicInteger tryCount = new AtomicInteger(0);
		cbTmpl.insertById(Person.class).one(person);
		for (int i = 0; i < 50; i++) { // the transaction sometimes succeeds on the first try
			ReplaceLoopThread t = new ReplaceLoopThread(switchedPerson);
			t.start();
			tryCount.set(0);
			TransactionsWrapper transactionsWrapper = new TransactionsWrapper(couchbaseClientFactory);
			TransactionResult txResult = transactionsWrapper.run(ctx -> {
				System.err.println("try: " + tryCount.incrementAndGet());
				Person ppp = cbTmpl.findById(Person.class).one(person.getId().toString());
				Person pppp = cbTmpl.replaceById(Person.class).one(ppp);
			});
			System.out.println("txResult: " + txResult);
			t.setStopFlag();
			if (tryCount.get() > 1) {
				break;
			}
		}
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
	}


	/**
	 * This does process retries - by CallbackTransactionManager.execute() -> transactions.run() -> executeTransaction()
	 * -> retryWhen.
	 */
	@Test
	public void replaceWithCasConflictResolvedViaRetryAnnotatedCallback() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		AtomicInteger tryCount = new AtomicInteger(0);
		cbTmpl.insertById(Person.class).one(person);
		for (int i = 0; i < 50; i++) { // the transaction sometimes succeeds on the first try
			ReplaceLoopThread t = new ReplaceLoopThread(switchedPerson);
			t.start();
			tryCount.set(0);
			Person p = personService.declarativeFindReplacePersonCallback(switchedPerson, tryCount);
			t.setStopFlag();
			if (tryCount.get() > 1) {
				break;
			}
		}
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
	}

	/**
	 * Reactive @Transactional does not retry write-write conflicts. It throws RetryTransactionException up to the client
	 * and expects the client to retry.
	 */
	@Test
	public void replaceWithCasConflictResolvedViaRetryAnnotatedReactive() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		cbTmpl.insertById(Person.class).one(person);
		AtomicInteger tryCount = new AtomicInteger(0);
		for (int i = 0; i < 50; i++) { // the transaction sometimes succeeds on the first try
			ReplaceLoopThread t = new ReplaceLoopThread(switchedPerson);
			t.start();
			tryCount.set(0);
			// TODO mr - Graham says not to do delegate retries to user. He's a party pooper.
			Person res = personService.declarativeFindReplacePersonReactive(switchedPerson, tryCount)
					.retryWhen(Retry.backoff(10, Duration.ofMillis(50))
							.filter(throwable -> throwable instanceof TransactionOperationFailedException)
							.onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
								throw new RuntimeException("Transaction failed  after max retries");
							}))
					.block();
			t.setStopFlag();
			if (tryCount.get() > 1) {
				break;
			}
		}
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
	}

	@Test
	/**
	 * This fails with TransactionOperationFailed {ec:FAIL_CAS_MISMATCH, retry:true, autoRollback:true}. I don't know why
	 * it isn't retried. This seems like it is due to the functioning of AbstractPlatformTransactionManager
	 */
	public void replaceWithCasConflictResolvedViaRetryAnnotated() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		cbTmpl.insertById(Person.class).one(person);
		AtomicInteger tryCount = new AtomicInteger(0);

		for (int i = 0; i < 50; i++) { // the transaction sometimes succeeds on the first try
			ReplaceLoopThread t = new ReplaceLoopThread(switchedPerson);
			t.start();
			tryCount.set(0);
			Person p = personService.declarativeFindReplacePerson(person, tryCount);
			t.setStopFlag();
			if (tryCount.get() > 1) {
				break;
			}
		}
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		System.out.println("pFound: " + pFound);
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
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

	private <T> Mono<T> throwSimulatedFailure(T p) {
		throw new SimulateFailureException();
	}

	@Data
	static class EventLog {

		public EventLog(){};

		public EventLog(ObjectId oid, String action) {
			this.id = oid.toString();
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

	@Service
	@Component
	@EnableTransactionManagement
	static class PersonService {

		final CouchbaseOperations personOperations;
		final CouchbaseSimpleCallbackTransactionManager manager; // final ReactiveCouchbaseTransactionManager manager;
		final ReactiveCouchbaseOperations personOperationsRx;
		final ReactiveCouchbaseTransactionManager managerRx;

		public PersonService(CouchbaseOperations ops, CouchbaseSimpleCallbackTransactionManager mgr,
							 ReactiveCouchbaseOperations opsRx, ReactiveCouchbaseTransactionManager mgrRx) {
			personOperations = ops;
			manager = mgr;
			personOperationsRx = opsRx;
			managerRx = mgrRx;
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
			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
					new DefaultTransactionDefinition());
			return personOperationsRx.insertById(Person.class).one(person)//
					.then(personOperationsRx.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count())
					.as(transactionalOperator::transactional).block();
		}

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
		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public Person declarativeSavePerson(Person person) {
			assertInAnnotationTransaction(true);
			return personOperations.insertById(Person.class).one(person);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
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
		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public Person declarativeFindReplacePersonCallback(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			System.err.println("declarativeFindReplacePersonCallback try: " + tryCount.incrementAndGet());
			Person p = personOperations.findById(Person.class).one(person.getId().toString());
			return personOperations.replaceById(Person.class).one(p.withFirstName(person.getFirstname()));
		}

		/**
		 * The ReactiveCouchbaseTransactionManager does not retry on write-write conflict. Instead it will throw
		 * RetryTransactionException to execute while ThreadReplaceloop() is running should force a retry
		 *
		 * @param person
		 * @return
		 */
		@Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)
		public Mono<Person> declarativeFindReplacePersonReactive(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			System.err.println("declarativeFindReplacePersonReactive try: " + tryCount.incrementAndGet());
			return personOperationsRx.findById(Person.class).one(person.getId().toString())
					.flatMap(p -> personOperationsRx.replaceById(Person.class).one(p.withFirstName(person.getFirstname())));
		}

		/**
		 * to execute while ThreadReplaceloop() is running should force a retry
		 *
		 * @param person
		 * @return
		 */
		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
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

		@Bean
		public Cluster couchbaseCluster() {
			return Cluster.connect("10.144.220.101", "Administrator", "password");
		}

	}

}
