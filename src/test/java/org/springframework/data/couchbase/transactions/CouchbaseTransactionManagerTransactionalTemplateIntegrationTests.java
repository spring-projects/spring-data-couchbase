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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertInTransaction;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import com.couchbase.client.java.transactions.error.TransactionFailedException;

/**
 * Tests for @Transactional, using the CouchbaseTransactionManager.
 * Some of these tests probably redundantly overlap with those elsewhere - but not all, since some of these fail.
 * These tests fail, and are known to fail.  I'm adding them to demonstrate why I don't feel we can have this
 * CouchbaseTransactionManager: it doesn't provide the crucial 'core loop' functionality, including error handling
 * and retries.  We should standardise on CouchbaseSimpleCallbackTransactionManager instead.
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(
		classes = { TransactionsConfigCouchbaseTransactionManager.class, CouchbaseTransactionManagerTransactionalTemplateIntegrationTests.PersonService.class })
public class CouchbaseTransactionManagerTransactionalTemplateIntegrationTests extends JavaIntegrationTests {
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
		assertNotInTransaction();
	}

	@AfterEach
	public void afterEachTest() {
		assertNotInTransaction();
	}

	@DisplayName("A basic golden path insert should succeed")
	@Test
	public void committedInsert() {
		UUID id = UUID.randomUUID();
		AtomicInteger tryCount = new AtomicInteger(0);

		Person inserted = personService.doInTransaction(tryCount, (ops) -> {
			Person person = new Person(id, "Walter", "White");
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
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
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
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		operations.insertById(Person.class).one(person);

		personService.fetchAndRemove(person.getId().toString(), tryCount);

		Person fetched = operations.findById(Person.class).one(person.getId().toString());
		assertNull(fetched);
		assertEquals(1, tryCount.get());
	}

	// gp: this test fails because it raises SimulateFailureException directly, rather than TransactionFailedException.
	// This is because CouchbaseTransactionManager is not doing (and probably cannot do) the requisite error handling.
	@DisplayName("Basic test of doing an insert then rolling back")
	@Test
	public void rollbackInsert() {
		AtomicInteger tryCount = new AtomicInteger(0);
		UUID id = UUID.randomUUID();

		try {
			personService.doInTransaction(tryCount, (ops) -> {
				Person person = new Person(id, "Walter", "White");
				ops.insertById(Person.class).one(person);
				throw new SimulateFailureException();
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		Person fetched = operations.findById(Person.class).one(id.toString());
		assertNull(fetched);
		assertEquals(1, tryCount.get());
	}

	// gp: this test is failing for same reason as rollbackInsert
	@DisplayName("Basic test of doing a replace then rolling back")
	@Test
	public void rollbackReplace() {
		AtomicInteger tryCount = new AtomicInteger(0);
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		operations.insertById(Person.class).one(person);

		try {
			personService.doInTransaction(tryCount, (ops) -> {
				Person p = ops.findById(Person.class).one(person.getId().toString());
				p.setFirstname("changed");
				ops.replaceById(Person.class).one(p);
				throw new SimulateFailureException();
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		Person fetched = operations.findById(Person.class).one(person.getId().toString());
		assertEquals("Walter", fetched.getFirstname());
		assertEquals(1, tryCount.get());
	}

	// gp: this test is failing for same reason as rollbackInsert
	@DisplayName("Basic test of doing a remove then rolling back")
	@Test
	public void rollbackRemove() {
		AtomicInteger tryCount = new AtomicInteger(0);
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		operations.insertById(Person.class).one(person);

		try {
			personService.doInTransaction(tryCount, (ops) -> {
				Person p = ops.findById(Person.class).one(person.getId().toString());
				ops.removeById(Person.class).oneEntity(p);
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

	// gp: this test is failing for same reason as rollbackInsert
	@DisplayName("Basic test of doing a removeByQuery then rolling back")
	@Test
	public void rollbackRemoveByQuery() {
		AtomicInteger tryCount = new AtomicInteger(0);
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		operations.insertById(Person.class).one(person);

		try {
			personService.doInTransaction(tryCount, ops -> {
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

	// gp: this test is failing for same reason as rollbackInsert
	@DisplayName("Basic test of doing a findByQuery then rolling back")
	@Test
	public void rollbackFindByQuery() {
		AtomicInteger tryCount = new AtomicInteger(0);
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
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

	// gp: this test fails because it's not retrying - because CouchbaseTransactionManager is not doing (and probably
	// cannot do) the requisite error handling and retries.
	@DisplayName("Forcing CAS mismatch causes a transaction retry")
	@Test
	public void casMismatchCausesRetry() {
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		operations.insertById(Person.class).one(person);
		AtomicInteger attempts = new AtomicInteger();

		// Needs to take place in a separate thread to bypass the ThreadLocalStorage checks
		Thread forceCASMismatch = new Thread(() -> {
			Person fetched = operations.findById(Person.class).one(id.toString());
			operations.replaceById(Person.class).one(fetched.withFirstName("Changed externally"));
		});

		personService.doInTransaction(attempts, ctx -> {
			Person fetched = operations.findById(Person.class).one(id.toString());

			if (attempts.get() == 1) {
				forceCASMismatch.start();
				try {
					forceCASMismatch.join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

			return operations.replaceById(Person.class).one(fetched.withFirstName("Changed by transaction"));
		});

		Person fetched = operations.findById(Person.class).one(person.getId().toString());
		assertEquals("Changed by transaction", fetched.getFirstname());
		assertEquals(2, attempts.get());
	}

	@Service
	@Component
	@EnableTransactionManagement
	static class PersonService {
		final CouchbaseOperations personOperations;

		public PersonService(CouchbaseOperations ops) {
			personOperations = ops;
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER, timeout = 2)
		public Person replace(Person person, AtomicInteger tryCount) {
			assertInTransaction();
			tryCount.incrementAndGet();
			return personOperations.replaceById(Person.class).one(person);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
		public Person fetchAndReplace(String id, AtomicInteger tryCount, Function<Person, Person> callback) {
			assertInTransaction();
			tryCount.incrementAndGet();
			Person p = personOperations.findById(Person.class).one(id);
			Person modified = callback.apply(p);
			return personOperations.replaceById(Person.class).one(modified);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
		public <T> T doInTransaction(AtomicInteger tryCount, Function<CouchbaseOperations, T> callback) {
			assertInTransaction();
			tryCount.incrementAndGet();
			return callback.apply(personOperations);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
		public void fetchAndRemove(String id, AtomicInteger tryCount) {
			assertInTransaction();
			tryCount.incrementAndGet();
			Person p = personOperations.findById(Person.class).one(id);
			personOperations.removeById(Person.class).oneEntity(p);
		}
	}
}
