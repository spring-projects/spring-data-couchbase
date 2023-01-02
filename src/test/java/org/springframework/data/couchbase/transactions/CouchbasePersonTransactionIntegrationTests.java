/*
 * Copyright 2022-2023 the original author or authors
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
import static org.junit.jupiter.api.Assertions.assertTrue;

import lombok.Data;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.TransactionalSupport;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
import org.springframework.data.couchbase.transaction.error.TransactionSystemUnambiguousException;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.reactive.TransactionalOperator;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.java.transactions.TransactionResult;

/**
 * Tests for com.couchbase.transactions using
 * <li><le>couchbase reactive transaction manager via transactional operator</le> <le>couchbase non-reactive transaction
 * manager via @Transactional</le> <le>@Transactional(transactionManager =
 * BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)</le></li>
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(classes = { TransactionsConfig.class, PersonService.class })
public class CouchbasePersonTransactionIntegrationTests extends JavaIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired PersonRepository repo;
	@Autowired ReactivePersonRepository rxRepo;
	@Autowired CouchbaseTemplate cbTmpl;
	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;
	@Autowired PersonService personService;
	@Autowired TransactionalOperator transactionalOperator;

	String sName = "_default";
	String cName = "_default";

	Person WalterWhite;

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
		WalterWhite = new Person("Walter", "White");
		TransactionTestUtil.assertNotInTransaction();
		List<RemoveResult> rp0 = cbTmpl.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<RemoveResult> rp1 = cbTmpl.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).inScope(sName)
				.inCollection(cName).all();
		List<RemoveResult> rp2 = cbTmpl.removeByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all();
		List<RemoveResult> rp3 = cbTmpl.removeByQuery(EventLog.class).withConsistency(REQUEST_PLUS).inScope(sName)
				.inCollection(cName).all();

		List<Person> p0 = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<Person> p1 = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).inScope(sName).inCollection(cName)
				.all();
		List<EventLog> e0 = cbTmpl.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all();
		List<EventLog> e1 = cbTmpl.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).inScope(sName)
				.inCollection(cName).all();

	}

	@DisplayName("rollback after exception using transactionalOperator")
	@Test
	public void shouldRollbackAfterException() {
		assertThrowsWithCause(() -> personService.savePersonErrors(WalterWhite),
				TransactionSystemUnambiguousException.class, SimulateFailureException.class);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	@DisplayName("rollback after exception using @Transactional")
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethod() {
		assertThrowsWithCause(() -> personService.declarativeSavePersonErrors(WalterWhite),
				TransactionSystemUnambiguousException.class, SimulateFailureException.class);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	@DisplayName("rollback after exception after using @Transactional(reactive)")
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethodReactive() {
		assertThrowsWithCause(() -> personService.declarativeSavePersonErrorsReactive(WalterWhite).block(),
				TransactionSystemUnambiguousException.class, SimulateFailureException.class);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	public void commitShouldPersistTxEntries() {
		Person p = personService.savePerson(WalterWhite);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	public void commitShouldPersistTxEntriesOfTxAnnotatedMethod() {
		Person p = personService.declarativeSavePerson(WalterWhite);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	/**
	 * This fails with TransactionOperationFailedException {ec:FAIL_CAS_MISMATCH, retry:true, autoRollback:true}. I don't
	 * know why it isn't retried. This seems like it is due to the functioning of AbstractPlatformTransactionManager
	 */
	public void replaceInTxAnnotatedCallback() {
		Person person = cbTmpl.insertById(Person.class).one(WalterWhite);
		Person switchedPerson = new Person(person.getId(), "Dave", "Reynolds");

		AtomicInteger tryCount = new AtomicInteger(0);
		Person p = personService.declarativeFindReplacePersonCallback(switchedPerson, tryCount);
		Person pFound = cbTmpl.findById(Person.class).one(person.id());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
	}

	@Test
	public void commitShouldPersistTxEntriesOfTxAnnotatedMethodReactive() {
		Person p = personService.declarativeSavePersonReactive(WalterWhite).block();
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	public void commitShouldPersistTxEntriesAcrossCollections() {
		List<EventLog> persons = personService.saveWithLogs(WalterWhite);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
		Long countEvents = cbTmpl.count(new Query(), EventLog.class); //
		assertEquals(4, countEvents, "should have saved and found 4");
	}

	@Test
	public void rollbackShouldAbortAcrossCollections() {
		assertThrowsWithCause(() -> personService.saveWithErrorLogs(WalterWhite),
				TransactionSystemUnambiguousException.class, SimulateFailureException.class);
		List<Person> persons = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		assertEquals(0, persons.size(), "should have done roll back and left 0 entries");
		List<EventLog> events = cbTmpl.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all(); //
		assertEquals(0, events.size(), "should have done roll back and left 0 entries");
	}

	@Test
	public void countShouldWorkInsideTransaction() {
		Long count = personService.countDuringTx(WalterWhite);
		assertEquals(1, count, "should have counted 1 during tx");
	}

	@Test
	public void emitMultipleElementsDuringTransaction() {
		List<EventLog> docs = personService.saveWithLogs(WalterWhite);
		assertEquals(4, docs.size(), "should have found 4 eventlogs");
	}

	@Test
	public void errorAfterTxShouldNotAffectPreviousStep() {
		Person p = personService.savePerson(WalterWhite);
		assertThrowsOneOf(() -> personService.savePerson(p), TransactionSystemUnambiguousException.class,
				DocumentExistsException.class);
		Long count = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	public void replacePersonCBTransactionsRxTmpl() {
		Person person = cbTmpl.insertById(Person.class).one(WalterWhite);
		Mono<Person> result = rxCBTmpl.findById(Person.class).one(person.id()) //
				.flatMap(pp -> rxCBTmpl.replaceById(Person.class).one(pp)).doOnNext(ppp -> TransactionalSupport
						.checkForTransactionInThreadLocalStorage().doOnNext(v -> assertTrue(v.isPresent())))
				.as(transactionalOperator::transactional);
		result.block();
		Person pFound = cbTmpl.findById(Person.class).one(person.id());
		assertEquals(person, pFound, "should have found expected " + person);
	}

	@Test
	public void insertPersonCBTransactionsRxTmplRollback() {
		Mono<Person> result = rxCBTmpl.insertById(Person.class).one(WalterWhite) //
				.doOnNext(ppp -> TransactionalSupport.checkForTransactionInThreadLocalStorage()
						.doOnNext(v -> assertTrue(v.isPresent())))
				.map(p -> throwSimulateFailureException(p)).as(transactionalOperator::transactional); // tx
		assertThrowsWithCause(result::block, TransactionSystemUnambiguousException.class, SimulateFailureException.class);
		Person pFound = cbTmpl.findById(Person.class).one(WalterWhite.id());
		assertNull(pFound, "insert should have been rolled back");
	}

	@Test
	public void insertTwicePersonCBTransactionsRxTmplRollback() {
		Mono<Person> result = rxCBTmpl.insertById(Person.class).one(WalterWhite) //
				.flatMap(ppp -> rxCBTmpl.insertById(Person.class).one(ppp)) //
				.as(transactionalOperator::transactional);
		assertThrowsWithCause(result::block, TransactionSystemUnambiguousException.class, DuplicateKeyException.class);
		Person pFound = cbTmpl.findById(Person.class).one(WalterWhite.id());
		assertNull(pFound, "insert should have been rolled back");
	}

	/**
	 * I think this test might fail sometimes? Does it need retryWhen() ?
	 */
	@Disabled("todo gp: disabling temporarily as hanging intermittently")
	@Test
	public void wrapperReplaceWithCasConflictResolvedViaRetry() {
		AtomicInteger tryCount = new AtomicInteger();
		Person person = cbTmpl.insertById(Person.class).one(WalterWhite);
		String newName = "Dave";

		TransactionResult txResult = couchbaseClientFactory.getCluster().transactions().run(ctx -> {
			Person ppp = cbTmpl.findById(Person.class).one(person.id());
			ReplaceLoopThread.updateOutOfTransaction(cbTmpl, person, tryCount.incrementAndGet());
			Person pppp = cbTmpl.replaceById(Person.class).one(ppp.withFirstName(newName));
		});

		Person pFound = cbTmpl.findById(Person.class).one(person.id());
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
		assertEquals(newName, pFound.getFirstname(), "should have been switched");
	}

	/**
	 * This does process retries - by CallbackTransactionManager.execute() -> transactions.run() -> executeTransaction()
	 * -> retryWhen.
	 */
	/**
	 * This fails with TransactionOperationFailedException {ec:FAIL_CAS_MISMATCH, retry:true, autoRollback:true}. I don't
	 * know why it isn't retried. This seems like it is due to the functioning of AbstractPlatformTransactionManager
	 */
	@Test
	public void replaceWithCasConflictResolvedViaRetryAnnotatedCallback() {
		Person person = cbTmpl.insertById(Person.class).one(WalterWhite);
		Person switchedPerson = new Person(person.getId(), "Dave", "Reynolds");
		AtomicInteger tryCount = new AtomicInteger();

		Person p = personService.declarativeFindReplacePersonCallback(switchedPerson, tryCount);
		Person pFound = cbTmpl.findById(Person.class).one(person.id());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
	}

	/**
	 * Reactive @Transactional does not retry write-write conflicts. It throws RetryTransactionException up to the client
	 * and expects the client to retry.
	 */
	@Test
	public void replaceWithCasConflictResolvedViaRetryAnnotatedReactive() {
		Person person = cbTmpl.insertById(Person.class).one(WalterWhite);
		Person switchedPerson = new Person(person.getId(), "Dave", "Reynolds");
		AtomicInteger tryCount = new AtomicInteger();

		Person res = personService.declarativeFindReplacePersonReactive(switchedPerson, tryCount).block();

		Person pFound = cbTmpl.findById(Person.class).one(person.id());
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
	}

	@Test
	public void replaceWithCasConflictResolvedViaRetryAnnotated() {
		Person person = cbTmpl.insertById(Person.class).one(WalterWhite);
		Person switchedPerson = person.withFirstName("Dave");
		AtomicInteger tryCount = new AtomicInteger();
		Person p = personService.declarativeFindReplacePerson(switchedPerson, tryCount);
		Person pFound = cbTmpl.findById(Person.class).one(person.id());
		System.out.println("pFound: " + pFound);
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
		assertTrue(tryCount.get() > 1, "should have been more than one try. tries: " + tryCount.get());
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
