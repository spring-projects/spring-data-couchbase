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

import lombok.Data;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Disabled;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
import org.springframework.data.couchbase.transaction.TransactionsWrapper;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.reactive.TransactionalOperator;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.RemoveOptions;
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
@SpringJUnitConfig(classes = { TransactionsConfigCouchbaseTransactionManager.class, PersonService.class })
public class CouchbasePersonTransactionIntegrationTests extends JavaIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory;
	@Autowired ReactiveCouchbaseTransactionManager reactiveCouchbaseTransactionManager;
	@Autowired PersonRepository repo;
	@Autowired ReactivePersonRepository rxRepo;
	@Autowired CouchbaseTemplate cbTmpl;
	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;
	@Autowired PersonService personService;
	@Autowired TransactionalOperator transactionalOperator;

	// if these are changed from default, then beforeEach needs to clean up separately
	String sName = "_default";
	String cName = "_default";

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
		List<RemoveResult> rp0 = cbTmpl.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<RemoveResult> rp1 = cbTmpl.removeByQuery(Person.class).inScope(sName).inCollection(cName)
				.withConsistency(REQUEST_PLUS).all();
		List<RemoveResult> rp2 = cbTmpl.removeByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all();
		List<RemoveResult> rp3 = cbTmpl.removeByQuery(EventLog.class).inScope(sName).inCollection(cName)
				.withConsistency(REQUEST_PLUS).all();

		List<Person> p0 = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<Person> p1 = cbTmpl.findByQuery(Person.class).inScope(sName).inCollection(cName).withConsistency(REQUEST_PLUS)
				.all();
		List<EventLog> e0 = cbTmpl.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all();
		List<EventLog> e1 = cbTmpl.findByQuery(EventLog.class).inScope(sName).inCollection(cName)
				.withConsistency(REQUEST_PLUS).all();

	}

	@DisplayName("rollback after exception using transactionalOperator")
	@Test
	public void shouldRollbackAfterException() {
		Person p = new Person( "Walter", "White");
		assertThrowsWithCause(() -> personService.savePersonErrors(p),
				SimulateFailureException.class);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	@DisplayName("rollback after exception using @Transactional")
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethod() {
		Person p = new Person("Walter", "White");
		assertThrowsWithCause(() -> personService.declarativeSavePersonErrors(p),  SimulateFailureException.class);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	@DisplayName("rollback after exception after using @Transactional(reactive)")
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethodReactive() {
		Person p = new Person("Walter", "White");
		assertThrowsWithCause(() -> personService.declarativeSavePersonErrorsReactive(p).block(),
				SimulateFailureException.class);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	public void commitShouldPersistTxEntries() {
		Person p = new Person("Walter", "White");
		Person s = personService.savePerson(p);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	public void commitShouldPersistTxEntriesOfTxAnnotatedMethod() {
		Person p = new Person( "Walter", "White");
		Person s = personService.declarativeSavePerson(p);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	/**
	 * This fails with TransactionOperationFailedException {ec:FAIL_CAS_MISMATCH, retry:true, autoRollback:true}. I don't know why
	 * it isn't retried. This seems like it is due to the functioning of AbstractPlatformTransactionManager
	 */
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
		Person p = new Person( "Walter", "White");
		Person s = personService.declarativeSavePersonReactive(p).block();
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	public void commitShouldPersistTxEntriesAcrossCollections() {
		List<EventLog> persons = personService.saveWithLogs(new Person ("Walter", "White"));
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
		Long countEvents = cbTmpl.count(new Query(), EventLog.class); //
		assertEquals(4, countEvents, "should have saved and found 4");
	}

	// @Disabled("gp: as CouchbaseTransactionOperation or TransactionalOperator user")
	@Test
	public void rollbackShouldAbortAcrossCollections() {
		assertThrowsWithCause(() -> personService.saveWithErrorLogs(new Person("Walter", "White")),
				SimulateFailureException.class);
		List<Person> persons = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		assertEquals(0, persons.size(), "should have done roll back and left 0 entries");
		List<EventLog> events = cbTmpl.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all(); //
		assertEquals(0, events.size(), "should have done roll back and left 0 entries");
	}

	@Test
	public void countShouldWorkInsideTransaction() {
		Long count = personService.countDuringTx(new Person( "Walter", "White"));
		assertEquals(1, count, "should have counted 1 during tx");
	}

	// @Disabled("gp: as CouchbaseTransactionOperation or TransactionalOperator user")
	@Test
	public void emitMultipleElementsDuringTransaction() {
		List<EventLog> docs = personService.saveWithLogs(new Person("Walter", "White"));
		assertEquals(4, docs.size(), "should have found 4 eventlogs");
	}

	// @Disabled("gp: as CouchbaseTransactionOperation or TransactionalOperator user")
	@Test
	public void errorAfterTxShouldNotAffectPreviousStep() {
		Person p = personService.savePerson(new Person( "Walter", "White"));
		// todo gp user shouldn't be getting exposed to TransactionOperationFailedException. This is happening as
		// TransactionOperator does not support the automatic retries and error handling we depend on. As argued on Slack,
		// we cannot support it because of this.
		// todo mr
		/*
		TransactionOperationFailedException {cause:com.couchbase.client.core.error.DocumentExistsException, retry:false, autoRollback:true, raise:TRANSACTION_FAILED}
		at com.couchbase.client.core.error.transaction.TransactionOperationFailedException$Builder.build(TransactionOperationFailedException.java:136)
		at com.couchbase.client.core.transaction.CoreTransactionAttemptContext.lambda$handleDocExistsDuringStagedInsert$116(CoreTransactionAttemptContext.java:1801)
		 */
		assertThrowsOneOf(() -> personService.savePerson(p), TransactionOperationFailedException.class);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	public void replacePersonCBTransactionsRxTmpl() {
		Person person = new Person(1, "Walter", "White");
		cbTmpl.insertById(Person.class).one(person);
		Mono<Person> result = rxCBTmpl.findById(Person.class).one(person.getId().toString()) //
				.flatMap(pp -> rxCBTmpl.replaceById(Person.class).one(pp)).flatMap(ppp -> assertInReactiveTransaction(ppp))
				.as(transactionalOperator::transactional);
		result.block();
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertEquals(person, pFound, "should have found expected " + person);
	}

	// @Disabled("gp: as CouchbaseTransactionOperation or TransactionalOperator user")
	@Test
	public void insertPersonCBTransactionsRxTmplRollback() {
		Person person = new Person(1, "Walter", "White");
		Mono<Person> result = rxCBTmpl.insertById(Person.class).one(person) //
				.flatMap(ppp -> assertInReactiveTransaction(ppp)).map(p -> throwSimulateFailureException(p))
				.as(transactionalOperator::transactional); // tx
		assertThrowsWithCause(result::block, SimulateFailureException.class);
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertNull(pFound, "insert should have been rolled back");
	}

	// @Disabled("gp: as CouchbaseTransactionOperation or TransactionalOperator user")
	@Test
	public void insertTwicePersonCBTransactionsRxTmplRollback() {
		Person person = new Person(1, "Walter", "White");
		Mono<Person> result = rxCBTmpl.insertById(Person.class).one(person) //
				.flatMap(ppp -> rxCBTmpl.insertById(Person.class).one(ppp)) //
				.as(transactionalOperator::transactional);
		assertThrowsWithCause(result::block,  DuplicateKeyException.class);
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertNull(pFound, "insert should have been rolled back");
	}

	/**
	 * I think this test might fail sometimes? Does it need retryWhen() ?
	 */
	@Disabled("todo gp: disabling temporarily as hanging intermittently")
	@Test
	public void wrapperReplaceWithCasConflictResolvedViaRetry() {
		Person person = new Person(1, "Walter", "White");
		AtomicInteger tryCount = new AtomicInteger(0);
		Person insertedPerson = cbTmpl.insertById(Person.class).one(person);
		String newName = "Dave";

		tryCount.set(0);
		TransactionsWrapper transactionsWrapper = new TransactionsWrapper(couchbaseClientFactory);
		TransactionResult txResult = transactionsWrapper.run(ctx -> {
			Person ppp = cbTmpl.findById(Person.class).one(person.getId().toString());
			ReplaceLoopThread.updateOutOfTransaction(cbTmpl, insertedPerson, tryCount.incrementAndGet());
			Person pppp = cbTmpl.replaceById(Person.class).one(ppp.withFirstName(newName));
		});

		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
		assertEquals(newName, pFound.getFirstname(), "should have been switched");
	}

	/**
	 * This does process retries - by CallbackTransactionManager.execute() -> transactions.run() -> executeTransaction()
	 * -> retryWhen.
	 */
	/**
	 * This fails with TransactionOperationFailedException {ec:FAIL_CAS_MISMATCH, retry:true, autoRollback:true}. I don't know why
	 * it isn't retried. This seems like it is due to the functioning of AbstractPlatformTransactionManager
	 */
	@Test
	public void replaceWithCasConflictResolvedViaRetryAnnotatedCallback() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		AtomicInteger tryCount = new AtomicInteger(0);
		cbTmpl.insertById(Person.class).one(person);
		tryCount.set(0);
		Person p = personService.declarativeFindReplacePersonCallback(switchedPerson, tryCount);
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
	}

	/**
	 * Reactive @Transactional does not retry write-write conflicts. It throws RetryTransactionException up to the client
	 * and expects the client to retry.
	 */
	// @Disabled("todo gp: disabled as failing and there's things to dig into here. This should not be raising
	// TransactionOperationFailedException for one")
	@Test
	public void replaceWithCasConflictResolvedViaRetryAnnotatedReactive() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		cbTmpl.insertById(Person.class).one(person);
		AtomicInteger tryCount = new AtomicInteger(0);

		// TODO mr - Graham says not to do delegate retries to user. He's a party pooper.
		Person res = personService.declarativeFindReplacePersonReactive(switchedPerson, tryCount)
				.retryWhen(Retry.backoff(10, Duration.ofMillis(500))
						.filter(throwable -> throwable instanceof OptimisticLockingFailureException)
						.filter(throwable -> throwable instanceof TransactionOperationFailedException)
						.onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
							throw new RuntimeException("Transaction failed  after max retries");
						}))
				.block();

		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
	}

	@Test
	/**
	 * This fails with TransactionOperationFailedException {ec:FAIL_CAS_MISMATCH, retry:true, autoRollback:true}. I don't know why
	 * it isn't retried. This seems like it is due to the functioning of AbstractPlatformTransactionManager
	 */
	public void replaceWithCasConflictResolvedViaRetryAnnotated() {
		Person person = new Person(1, "Walter", "White");
		Person insertedPerson = cbTmpl.insertById(Person.class).one(person);
		Person switchedPerson = insertedPerson.withFirstName("Dave");
		AtomicInteger tryCount = new AtomicInteger(0);
		Person p = personService.declarativeFindReplacePerson(switchedPerson, tryCount);
		Person pFound = cbTmpl.findById(Person.class).one(person.getId().toString());
		System.out.println("pFound: " + pFound);
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
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

	@Data
	static class EventLog {

		public EventLog() {}; // don't remove this

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

}
