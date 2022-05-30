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

import com.couchbase.client.java.transactions.error.TransactionFailedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonWithoutVersion;
import org.springframework.data.couchbase.transaction.CouchbaseSimpleCallbackTransactionManager;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import javax.management.Query;
import javax.management.ValueExp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

/**
 * Tests for @Transactional, using template methods (findById etc.)
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(classes = { Config.class, CouchbaseTransactionalTemplateIntegrationTests.PersonService.class} )
public class CouchbaseTransactionalTemplateIntegrationTests extends JavaIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired PersonService personService;
	@Autowired CouchbaseTemplate operations;

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
		// Skip this as we just one to track TransactionContext
		operations.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all(); // doesn't work???
		List<Person> p = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();

		Person walterWhite = new Person(1, "Walter", "White");
		try {
			couchbaseClientFactory.getBucket().defaultCollection().remove(walterWhite.getId().toString());
		} catch (Exception ex) {
			// System.err.println(ex);
		}
	}

	@AfterEach
	public void afterEachTest() {
		assertNotInTransaction();
	}

	@DisplayName("A basic golden path insert should succeed")
	@Test
	public void committedInsert() {
		AtomicInteger tryCount = new AtomicInteger(0);

		Person inserted = personService.doInTransaction(tryCount, (ops) -> {
			Person person = new Person(1, "Walter", "White");
			ops.insertById(Person.class).one(person);
			return person;
		});

		Person fetched = operations.findById(Person.class).one(inserted.getId().toString());
		assertEquals("Walter", fetched.getFirstname());
		assertEquals(1, tryCount.get());
	}

	@DisplayName("A basic golden path replace should succeed")
	@Test
	public void committedReplace() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = new Person(1, "Walter", "White");
		operations.insertById(Person.class).one(person);

		personService.fetchAndReplace(person.getId().toString(), tryCount, (p) -> {
			p.setFirstname("changed");
			return p;
		});

		Person fetched = operations.findById(Person.class).one(person.getId().toString());
		assertEquals("changed", fetched.getFirstname());
		assertEquals(1, tryCount.get());
	}

	@DisplayName("A basic golden path remove should succeed")
	@Test
	public void committedRemove() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = new Person(1, "Walter", "White");
		operations.insertById(Person.class).one(person);

		personService.fetchAndRemove(person.getId().toString(), tryCount);

		Person fetched = operations.findById(Person.class).one(person.getId().toString());
		assertNull(fetched);
		assertEquals(1, tryCount.get());
	}

	@DisplayName("A basic golden path removeByQuery should succeed")
	@Test
	public void committedRemoveByQuery() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = new Person(1, "Walter", "White");
		operations.insertById(Person.class).one(person);

		List<RemoveResult> removed = personService.doInTransaction(tryCount, ops -> {
			return ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq("Walter")).all();
		});

		Person fetched = operations.findById(Person.class).one(person.getId().toString());
		assertNull(fetched);
		assertEquals(1, tryCount.get());
		assertEquals(1, removed.size());
	}

	@DisplayName("A basic golden path findByQuery should succeed (though we don't know for sure it executed transactionally)")
	@Test
	public void committedFindByQuery() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = new Person(1, "Walter", "White");
		operations.insertById(Person.class).one(person);

		List<Person> found = personService.doInTransaction(tryCount, ops -> {
			return ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq("Walter")).all();
		});

		assertEquals(1, found.size());
	}

	@DisplayName("Basic test of doing an insert then rolling back")
	@Test
	public void rollbackInsert() {
		AtomicInteger tryCount = new AtomicInteger(0);
		AtomicReference<String> id = new AtomicReference<>();

		try {
			personService.doInTransaction(tryCount, (ops) -> {
				Person person = new Person(1, "Walter", "White");
				ops.insertById(Person.class).one(person);
				id.set(person.getId().toString());
				throw new SimulateFailureException();
			});
			fail();
		}
		catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		Person fetched = operations.findById(Person.class).one(id.get());
		assertNull(fetched);
		assertEquals(1, tryCount.get());
	}

	@DisplayName("Basic test of doing a replace then rolling back")
	@Test
	public void rollbackReplace() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = new Person(1, "Walter", "White");
		operations.insertById(Person.class).one(person);

		try {
			personService.doInTransaction(tryCount, (ops) -> {
				Person p = ops.findById(Person.class).one(person.getId().toString());
				p.setFirstname("changed");
				ops.replaceById(Person.class).one(p);
				throw new SimulateFailureException();
			});
			fail();
		}
		catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		Person fetched = operations.findById(Person.class).one(person.getId().toString());
		assertEquals("Walter", fetched.getFirstname());
		assertEquals(1, tryCount.get());
	}

	@DisplayName("Basic test of doing a remove then rolling back")
	@Test
	public void rollbackRemove() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = new Person(1, "Walter", "White");
		operations.insertById(Person.class).one(person);

		try {
			personService.doInTransaction(tryCount, (ops) -> {
				Person p = ops.findById(Person.class).one(person.getId().toString());
				ops.removeById(Person.class).oneEntity(p);
				throw new SimulateFailureException();
			});
			fail();
		}
		catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		Person fetched = operations.findById(Person.class).one(person.getId().toString());
		assertNotNull(fetched);
		assertEquals(1, tryCount.get());
	}

	@DisplayName("Basic test of doing a removeByQuery then rolling back")
	@Test
	public void rollbackRemoveByQuery() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = new Person(1, "Walter", "White");
		operations.insertById(Person.class).one(person);

		try {
			personService.doInTransaction(tryCount, ops -> {
				// todo gpx this isn't executed transactionally
				ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq("Walter")).all();
				throw new SimulateFailureException();
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		Person fetched = operations.findById(Person.class).one(person.getId().toString());
		assertNotNull(fetched);
		assertEquals(1, tryCount.get());
	}

	@DisplayName("Basic test of doing a findByQuery then rolling back")
	@Test
	public void rollbackFindByQuery() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = new Person(1, "Walter", "White");
		operations.insertById(Person.class).one(person);

		try {
			personService.doInTransaction(tryCount, ops -> {
				ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq("Walter")).all();
				throw new SimulateFailureException();
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		assertEquals(1, tryCount.get());
	}

	@Test
	public void shouldRollbackAfterException() {
		try {
			personService.insertThenThrow();
			fail();
		}
		catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	public void commitShouldPersistTxEntries() {
		Person p = new Person(null, "Walter", "White");
		Person s = personService.declarativeSavePerson(p);
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Disabled("because hanging - requires JCBC-1955 fix")
	@Test
	public void concurrentTxns() {
		Runnable r = () -> {
			Thread t = Thread.currentThread();
			System.out.printf("Started thread %d %s%n", t.getId(), t.getName());
			Person p = new Person(null, "Walter", "White");
			Person s = personService.declarativeSavePersonWithThread(p, t);
			System.out.printf("Finished thread %d %s%n", t.getId(), t.getName());
		};
		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 99; i ++) {
			Thread t = new Thread(r);
			t.start();
			threads.add(t);
		}
		threads.forEach(t -> {
			try {
				System.out.printf("Waiting for thread %d %s%n", t.getId(), t.getName());
				t.join();
				System.out.printf("Finished waiting for thread %d %s%n", t.getId(), t.getName());
			} catch (InterruptedException e) {
				fail();
			}
		});
	}

	// todo gpx investigate how @Transactional @Rollback/@Commit interacts with us

	@DisplayName("Create a Person outside a @Transactional block, modify it, and then replace that person in the @Transactional.  The transaction will retry until timeout.")
	@Test
	public void replacePerson() {
		Person person = new Person(1, "Walter", "White");
		operations.insertById(Person.class).one(person);

		Person refetched = operations.findById(Person.class).one(person.getId().toString());
		operations.replaceById(Person.class).one(refetched);

		assertNotEquals(person.getVersion(), refetched.getVersion());

		AtomicInteger tryCount = new AtomicInteger(0);
		try {
			personService.replace(person, tryCount);
			fail();
		}
		catch (TransactionFailedException ignored) {
		}
	}


	@DisplayName("Entity must have CAS field during replace")
	@Test
	public void replaceEntityWithoutCas() {
		PersonWithoutVersion person = new PersonWithoutVersion(UUID.randomUUID(), "Walter", "White");
		operations.insertById(PersonWithoutVersion.class).one(person);
		try {
			personService.replaceEntityWithoutVersion(person.getId().toString());
		}
		catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof IllegalArgumentException);
		}
	}

	@DisplayName("Entity must have non-zero CAS during replace")
	@Test
	public void replaceEntityWithCasZero() {
		Person person = new Person(1, "Walter", "White");
		operations.insertById(Person.class).one(person);

		// switchedPerson here will have CAS=0, which will fail
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		AtomicInteger tryCount = new AtomicInteger(0);

		try {
			personService.replacePerson(switchedPerson, tryCount);
		}
		catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof IllegalArgumentException);
		}
	}

	@DisplayName("Entity must have CAS field during remove")
	@Test
	public void removeEntityWithoutCas() {
		PersonWithoutVersion person = new PersonWithoutVersion(UUID.randomUUID(), "Walter", "White");
		operations.insertById(PersonWithoutVersion.class).one(person);
		try {
			personService.removeEntityWithoutVersion(person.getId().toString());
		}
		catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof IllegalArgumentException);
		}
	}

	@DisplayName("removeById().one(id) isn't allowed in transactions, since we don't have the CAS")
	@Test
	public void removeEntityById() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = new Person(1, "Walter", "White");
		operations.insertById(Person.class).one(person);

		try {
            personService.doInTransaction(tryCount, (ops) -> {
                Person p = ops.findById(Person.class).one(person.getId().toString());
                ops.removeById(Person.class).one(p.getId().toString());
                return p;
            });
            fail();
        }
		catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof IllegalArgumentException);
		}
	}

	@Service
	@Component
	@EnableTransactionManagement
	static
	class PersonService {
		final CouchbaseOperations personOperations;
		final ReactiveCouchbaseOperations personOperationsRx;

		public PersonService(CouchbaseOperations ops, ReactiveCouchbaseOperations opsRx) {
			personOperations = ops;
			personOperationsRx = opsRx;
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public Person declarativeSavePerson(Person person) {
			assertInAnnotationTransaction(true);
			long currentThreadId = Thread.currentThread().getId();
			System.out.println(String.format("Thread %d %s", Thread.currentThread().getId(), Thread.currentThread().getName()));
			Person ret = personOperations.insertById(Person.class).one(person);
			System.out.println(String.format("Thread %d (was %d) %s", Thread.currentThread().getId(), currentThreadId, Thread.currentThread().getName()));
			if (currentThreadId != Thread.currentThread().getId()) {
				throw new IllegalStateException();
			}
			return ret;
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public Person declarativeSavePersonWithThread(Person person, Thread thread) {
			assertInAnnotationTransaction(true);
			long currentThreadId = Thread.currentThread().getId();
			System.out.printf("Thread %d %s, started from %d %s%n", Thread.currentThread().getId(), Thread.currentThread().getName(), thread.getId(), thread.getName());
			Person ret = personOperations.insertById(Person.class).one(person);
			System.out.printf("Thread %d (was %d) %s, started from %d %s%n", Thread.currentThread().getId(), currentThreadId, Thread.currentThread().getName(), thread.getId(), thread.getName());
			if (currentThreadId != Thread.currentThread().getId()) {
				throw new IllegalStateException();
			}
			return ret;
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public void insertThenThrow() {
			Person person = new Person(null, "Walter", "White");
			assertInAnnotationTransaction(true);
			personOperations.insertById(Person.class).one(person);
			SimulateFailureException.throwEx();
		}

		@Autowired CouchbaseSimpleCallbackTransactionManager callbackTm;

		/**
		 * to execute while ThreadReplaceloop() is running should force a retry
		 *
		 * @param person
		 * @return
		 */
		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public Person replacePerson(Person person, AtomicInteger tryCount) {
			tryCount.incrementAndGet();
			// Note that passing in a Person and replace it in this way, is not supported
			return personOperations.replaceById(Person.class).one(person);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public void replaceEntityWithoutVersion(String id) {
			PersonWithoutVersion fetched = personOperations.findById(PersonWithoutVersion.class).one(id);
			personOperations.replaceById(PersonWithoutVersion.class).one(fetched);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public void removeEntityWithoutVersion(String id) {
			PersonWithoutVersion fetched = personOperations.findById(PersonWithoutVersion.class).one(id);
			personOperations.removeById(PersonWithoutVersion.class).oneEntity(fetched);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public Person declarativeFindReplaceTwicePersonCallback(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			System.err.println("declarativeFindReplacePersonCallback try: " + tryCount.incrementAndGet());
			Person p = personOperations.findById(Person.class).one(person.getId().toString());
			Person pUpdated = personOperations.replaceById(Person.class).one(p);
			return personOperations.replaceById(Person.class).one(pUpdated);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER, timeout = 2)
		public Person replace(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			tryCount.incrementAndGet();
			return personOperations.replaceById(Person.class).one(person);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public Person fetchAndReplace(String id, AtomicInteger tryCount, Function<Person, Person> callback) {
			assertInAnnotationTransaction(true);
			tryCount.incrementAndGet();
			Person p = personOperations.findById(Person.class).one(id);
			Person modified = callback.apply(p);
			return personOperations.replaceById(Person.class).one(modified);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public <T> T doInTransaction(AtomicInteger tryCount, Function<CouchbaseOperations, T> callback) {
			assertInAnnotationTransaction(true);
			tryCount.incrementAndGet();
			return callback.apply(personOperations);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public void fetchAndRemove(String id, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			tryCount.incrementAndGet();
			Person p = personOperations.findById(Person.class).one(id);
			personOperations.removeById(Person.class).oneEntity(p);
		}
	}

	static void assertInAnnotationTransaction(boolean inTransaction) {
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
