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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonWithoutVersion;
import org.springframework.data.couchbase.transaction.CouchbaseCallbackTransactionManager;
import org.springframework.data.couchbase.transaction.error.TransactionSystemUnambiguousException;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import com.couchbase.client.core.error.transaction.AttemptExpiredException;

/**
 * Tests for @Transactional, using template methods (findById etc.)
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(
		classes = { TransactionsConfig.class, CouchbaseTransactionalTemplateIntegrationTests.PersonService.class })
public class CouchbaseTransactionalTemplateIntegrationTests extends JavaIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired PersonService personService;
	@Autowired CouchbaseTemplate operations;

	Person WalterWhite;

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
		WalterWhite = new Person("Walter", "White");
		// Skip this as we just one to track TransactionContext
		List<RemoveResult> pr = operations.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<Person> p = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();

		List<RemoveResult> pwovr = operations.removeByQuery(PersonWithoutVersion.class).withConsistency(REQUEST_PLUS).all();
		List<PersonWithoutVersion> pwov = operations.findByQuery(PersonWithoutVersion.class).withConsistency(REQUEST_PLUS)
				.all();
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
			return ops.insertById(Person.class).one(WalterWhite.withIdFirstname());
		});

		Person fetched = operations.findById(Person.class).one(inserted.id());
		assertEquals(inserted.getFirstname(), fetched.getFirstname());
		assertEquals(1, tryCount.get());
	}

	@DisplayName("A basic golden path replace should succeed")
	@Test
	public void committedReplace() {
		AtomicInteger tryCount = new AtomicInteger();
		Person person = operations.insertById(Person.class).one(WalterWhite);

		personService.fetchAndReplace(person.id(), tryCount, (p) -> {
			p.setFirstname("changed");
			return p;
		});

		Person fetched = operations.findById(Person.class).one(person.id());
		assertEquals("changed", fetched.getFirstname());
		assertEquals(1, tryCount.get());
	}

	@DisplayName("A basic golden path remove should succeed")
	@Test
	public void committedRemove() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = operations.insertById(Person.class).one(WalterWhite);

		personService.fetchAndRemove(person.id(), tryCount);

		Person fetched = operations.findById(Person.class).one(person.id());
		assertNull(fetched);
		assertEquals(1, tryCount.get());
	}

	@DisplayName("A basic golden path removeByQuery should succeed")
	@Test
	public void committedRemoveByQuery() {
		AtomicInteger tryCount = new AtomicInteger();
		Person person = operations.insertById(Person.class).one(WalterWhite.withIdFirstname());

		List<RemoveResult> removed = personService.doInTransaction(tryCount, ops -> {
			return ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all();
		});

		Person fetched = operations.findById(Person.class).one(person.id());
		assertNull(fetched);
		assertEquals(1, tryCount.get());
		assertEquals(1, removed.size());
	}

	@DisplayName("A basic golden path findByQuery should succeed (though we don't know for sure it executed transactionally)")
	@Test
	public void committedFindByQuery() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = operations.insertById(Person.class).one(WalterWhite.withIdFirstname());

		List<Person> found = personService.doInTransaction(tryCount, ops -> {
			return ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all();
		});

		assertEquals(1, found.size());
	}

	@DisplayName("Basic test of doing an insert then rolling back")
	@Test
	public void rollbackInsert() {
		AtomicInteger tryCount = new AtomicInteger();
		AtomicReference<String> id = new AtomicReference<>();

		assertThrowsWithCause(() -> {
			personService.doInTransaction(tryCount, (ops) -> {
				ops.insertById(Person.class).one(WalterWhite);
				id.set(WalterWhite.id());
				throw new SimulateFailureException();
			});
		}, TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = operations.findById(Person.class).one(id.get());
		assertNull(fetched);
		assertEquals(1, tryCount.get());
	}

	@DisplayName("Basic test of doing a replace then rolling back")
	@Test
	public void rollbackReplace() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = operations.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> {
			personService.doInTransaction(tryCount, (ops) -> {
				Person p = ops.findById(Person.class).one(person.id());
				ops.replaceById(Person.class).one(p.withFirstName("changed"));
				throw new SimulateFailureException();
			});
		}, TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = operations.findById(Person.class).one(person.id());
		assertEquals(person.getFirstname(), fetched.getFirstname());
		assertEquals(1, tryCount.get());
	}

	@DisplayName("Basic test of doing a remove then rolling back")
	@Test
	public void rollbackRemove() {
		AtomicInteger tryCount = new AtomicInteger(0);
		Person person = operations.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> {
			personService.doInTransaction(tryCount, (ops) -> {
				Person p = ops.findById(Person.class).one(person.id());
				ops.removeById(Person.class).oneEntity(p);
				throw new SimulateFailureException();
			});
		}, TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = operations.findById(Person.class).one(person.id());
		assertNotNull(fetched);
		assertEquals(1, tryCount.get());
	}

	@DisplayName("Basic test of doing a removeByQuery then rolling back")
	@Test
	public void rollbackRemoveByQuery() {
		AtomicInteger tryCount = new AtomicInteger();
		Person person = operations.insertById(Person.class).one(WalterWhite.withIdFirstname());

		assertThrowsWithCause(() -> {
			personService.doInTransaction(tryCount, ops -> {
				ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all();
				throw new SimulateFailureException();
			});
		}, TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = operations.findById(Person.class).one(person.id());
		assertNotNull(fetched);
		assertEquals(1, tryCount.get());
	}

	@DisplayName("Basic test of doing a findByQuery then rolling back")
	@Test
	public void rollbackFindByQuery() {
		AtomicInteger tryCount = new AtomicInteger();
		Person person = operations.insertById(Person.class).one(WalterWhite.withIdFirstname());

		assertThrowsWithCause(() -> {
			personService.doInTransaction(tryCount, ops -> {
				ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all();
				throw new SimulateFailureException();
			});
		}, TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		assertEquals(1, tryCount.get());
	}

	@Test
	public void shouldRollbackAfterException() {
		assertThrowsWithCause(() -> {
			personService.insertThenThrow();
		}, TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	public void commitShouldPersistTxEntries() {
		Person p = personService.declarativeSavePerson(WalterWhite);
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test
	public void concurrentTxns() {
		Runnable r = () -> {
			Thread t = Thread.currentThread();
			System.out.printf("Started thread %d %s%n", t.getId(), t.getName());
			Person p = personService.declarativeSavePersonWithThread(WalterWhite, t);
			System.out.printf("Finished thread %d %s%n", t.getId(), t.getName());
		};
		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 50; i++) { // somewhere between 50-80 it starts to hang
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
				fail(); // interrupted
			}
		});
	}

	@DisplayName("Create a Person outside a @Transactional block, modify it, and then replace that person in the @Transactional.  The transaction will retry until timeout.")
	@Test
	public void replacePerson() {
		Person person = operations.insertById(Person.class).one(WalterWhite);
		Person refetched = operations.findById(Person.class).one(person.id());
		operations.replaceById(Person.class).one(refetched);
		assertNotEquals(person.getVersion(), refetched.getVersion());
		AtomicInteger tryCount = new AtomicInteger(0);
		assertThrowsWithCause(() -> personService.replace(person, tryCount), TransactionSystemUnambiguousException.class,
				AttemptExpiredException.class);
	}

	@DisplayName("Entity must have CAS field during replace")
	@Test
	public void replaceEntityWithoutCas() {
		PersonWithoutVersion person = new PersonWithoutVersion(UUID.randomUUID(), "Walter", "White");
		operations.insertById(PersonWithoutVersion.class).one(person);

		assertThrowsWithCause(() -> personService.replaceEntityWithoutVersion(person.id()),
				TransactionSystemUnambiguousException.class, IllegalArgumentException.class);
	}

	@DisplayName("Entity must have non-zero CAS during replace")
	@Test
	public void replaceEntityWithCasZero() {
		Person person = operations.insertById(Person.class).one(WalterWhite);
		// switchedPerson here will have CAS=0, which will fail
		Person switchedPerson = new Person("Dave", "Reynolds");
		AtomicInteger tryCount = new AtomicInteger(0);

		assertThrowsWithCause(() -> personService.replacePerson(switchedPerson, tryCount),
				TransactionSystemUnambiguousException.class, IllegalArgumentException.class);
	}

	@DisplayName("Entity must have CAS field during remove")
	@Test
	public void removeEntityWithoutCas() {
		PersonWithoutVersion person = new PersonWithoutVersion(UUID.randomUUID(), "Walter", "White");
		operations.insertById(PersonWithoutVersion.class).one(person);

		assertThrowsWithCause(() -> personService.removeEntityWithoutVersion(person.id()),
				TransactionSystemUnambiguousException.class, IllegalArgumentException.class);
	}

	@DisplayName("removeById().one(id) isn't allowed in transactions, since we don't have the CAS")
	@Test
	public void removeEntityById() {
		AtomicInteger tryCount = new AtomicInteger();
		Person person = operations.insertById(Person.class).one(WalterWhite);
		assertThrowsWithCause(() -> {
			personService.doInTransaction(tryCount, (ops) -> {
				Person p = ops.findById(Person.class).one(person.id());
				ops.removeById(Person.class).one(p.id());
				return p;
			});
		}, TransactionSystemUnambiguousException.class, IllegalArgumentException.class);
	}

	@Service // this will work in the unit tests even without @Service because of explicit loading by @SpringJUnitConfig
	static class PersonService {
		final CouchbaseOperations personOperations;
		final ReactiveCouchbaseOperations personOperationsRx;

		public PersonService(CouchbaseOperations ops, ReactiveCouchbaseOperations opsRx) {
			personOperations = ops;
			personOperationsRx = opsRx;
		}

		@Transactional
		public Person declarativeSavePerson(Person person) {
			assertInAnnotationTransaction(true);
			long currentThreadId = Thread.currentThread().getId();
			System.out
					.println(String.format("Thread %d %s", Thread.currentThread().getId(), Thread.currentThread().getName()));
			Person ret = personOperations.insertById(Person.class).one(person);
			System.out.println(String.format("Thread %d (was %d) %s", Thread.currentThread().getId(), currentThreadId,
					Thread.currentThread().getName()));
			if (currentThreadId != Thread.currentThread().getId()) {
				throw new IllegalStateException();
			}
			return ret;
		}

		@Transactional
		public Person declarativeSavePersonWithThread(Person person, Thread thread) {
			assertInAnnotationTransaction(true);
			long currentThreadId = Thread.currentThread().getId();
			System.out.printf("Thread %d %s, started from %d %s%n", Thread.currentThread().getId(),
					Thread.currentThread().getName(), thread.getId(), thread.getName());
			Person ret = personOperations.insertById(Person.class).one(person);
			System.out.printf("Thread %d (was %d) %s, started from %d %s%n", Thread.currentThread().getId(), currentThreadId,
					Thread.currentThread().getName(), thread.getId(), thread.getName());
			if (currentThreadId != Thread.currentThread().getId()) {
				throw new IllegalStateException();
			}
			return ret;
		}

		@Transactional
		public void insertThenThrow() {
			assertInAnnotationTransaction(true);
			Person person = personOperations.insertById(Person.class).one(new Person("Walter", "White"));
			SimulateFailureException.throwEx();
		}

		@Autowired CouchbaseCallbackTransactionManager callbackTm;

		/**
		 * to execute while ThreadReplaceloop() is running should force a retry
		 *
		 * @param person
		 * @return
		 */
		@Transactional
		public Person replacePerson(Person person, AtomicInteger tryCount) {
			tryCount.incrementAndGet();
			// Note that passing in a Person and replace it in this way, is not supported
			return personOperations.replaceById(Person.class).one(person);
		}

		@Transactional
		public void replaceEntityWithoutVersion(String id) {
			PersonWithoutVersion fetched = personOperations.findById(PersonWithoutVersion.class).one(id);
			personOperations.replaceById(PersonWithoutVersion.class).one(fetched);
		}

		@Transactional
		public void removeEntityWithoutVersion(String id) {
			PersonWithoutVersion fetched = personOperations.findById(PersonWithoutVersion.class).one(id);
			personOperations.removeById(PersonWithoutVersion.class).oneEntity(fetched);
		}

		@Transactional
		public Person declarativeFindReplaceTwicePersonCallback(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			System.err.println("declarativeFindReplacePersonCallback try: " + tryCount.incrementAndGet());
			Person p = personOperations.findById(Person.class).one(person.id());
			Person pUpdated = personOperations.replaceById(Person.class).one(p);
			return personOperations.replaceById(Person.class).one(pUpdated);
		}

		@Transactional(timeout = 2)

		public Person replace(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			tryCount.incrementAndGet();
			return personOperations.replaceById(Person.class).one(person);
		}

		@Transactional
		public Person fetchAndReplace(String id, AtomicInteger tryCount, Function<Person, Person> callback) {
			assertInAnnotationTransaction(true);
			tryCount.incrementAndGet();
			Person p = personOperations.findById(Person.class).one(id);
			Person modified = callback.apply(p);
			return personOperations.replaceById(Person.class).one(modified);
		}

		@Transactional
		public <T> T doInTransaction(AtomicInteger tryCount, Function<CouchbaseOperations, T> callback) {
			assertInAnnotationTransaction(true);
			tryCount.incrementAndGet();
			return callback.apply(personOperations);
		}

		@Transactional
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
