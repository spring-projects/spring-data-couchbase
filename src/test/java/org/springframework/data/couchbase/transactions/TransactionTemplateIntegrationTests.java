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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.TransactionalSupport;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonWithoutVersion;
import org.springframework.data.couchbase.transaction.CouchbaseSimpleCallbackTransactionManager;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import com.couchbase.client.java.transactions.error.TransactionFailedException;

/**
 * Tests for Spring's TransactionTemplate, used CouchbaseSimpleCallbackTransactionManager, using template methods
 * (findById etc.)
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(Config.class)
public class TransactionTemplateIntegrationTests extends JavaIntegrationTests {
	// todo gp can we get @AutoWired working here
	// @Autowired
	TransactionTemplate template;
	@Autowired CouchbaseSimpleCallbackTransactionManager transactionManager;
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired CouchbaseTemplate ops;

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

		template = new TransactionTemplate(transactionManager);
	}

	@AfterEach
	public void afterEachTest() {
		assertNotInTransaction();
	}

	static class RunResult {
		public final int attempts;

		public RunResult(int attempts) {
			this.attempts = attempts;
		}
	}

	private RunResult doInTransaction(Consumer<TransactionStatus> lambda) {
		AtomicInteger tryCount = new AtomicInteger(0);

		template.executeWithoutResult(status -> {
			assertFalse(status.hasSavepoint());
			assertFalse(status.isRollbackOnly());
			assertFalse(status.isCompleted());
			assertTrue(status.isNewTransaction());

			tryCount.incrementAndGet();
			lambda.accept(status);
		});

		return new RunResult(tryCount.get());
	}

	@DisplayName("A basic golden path insert should succeed")
	@Test
	public void committedInsert() {
		UUID id = UUID.randomUUID();

		RunResult rr = doInTransaction(status -> {
			Person person = new Person(id, "Walter", "White");
			ops.insertById(Person.class).one(person);
		});

		Person fetched = ops.findById(Person.class).one(id.toString());
		assertEquals("Walter", fetched.getFirstname());
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path replace should succeed")
	@Test
	public void committedReplace() {
		UUID id = UUID.randomUUID();
		Person initial = new Person(id, "Walter", "White");
		ops.insertById(Person.class).one(initial);

		RunResult rr = doInTransaction(status -> {
			Person person = ops.findById(Person.class).one(id.toString());
			person.setFirstname("changed");
			ops.replaceById(Person.class).one(person);
		});

		Person fetched = ops.findById(Person.class).one(initial.getId().toString());
		assertEquals("changed", fetched.getFirstname());
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path remove should succeed")
	@Test
	public void committedRemove() {
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		ops.insertById(Person.class).one(person);

		RunResult rr = doInTransaction(status -> {
			Person fetched = ops.findById(Person.class).one(id.toString());
			ops.removeById(Person.class).oneEntity(fetched);
		});

		Person fetched = ops.findById(Person.class).one(person.getId().toString());
		assertNull(fetched);
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path removeByQuery should succeed")
	@Test
	public void committedRemoveByQuery() {
		UUID id = UUID.randomUUID();
		Person person = new Person(id, id.toString(), "White");
		ops.insertById(Person.class).one(person);

		RunResult rr = doInTransaction(status -> {
			List<RemoveResult> removed = ops.removeByQuery(Person.class)
					.matching(QueryCriteria.where("firstname").eq(id.toString())).all();
			assertEquals(1, removed.size());
		});

		Person fetched = ops.findById(Person.class).one(person.getId().toString());
		assertNull(fetched);
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path findByQuery should succeed")
	@Test
	public void committedFindByQuery() {
		UUID id = UUID.randomUUID();
		Person person = new Person(id, id.toString(), "White");
		ops.insertById(Person.class).one(person);

		RunResult rr = doInTransaction(status -> {
			List<Person> found = ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(id.toString()))
					.all();
			assertEquals(1, found.size());
		});

		assertEquals(1, rr.attempts);
	}

	@DisplayName("Basic test of doing an insert then rolling back")
	@Test
	public void rollbackInsert() {
		AtomicInteger attempts = new AtomicInteger();
		UUID id = UUID.randomUUID();

		assertThrowsWithCause(() -> doInTransaction(status -> {
			attempts.incrementAndGet();
			Person person = new Person(id, "Walter", "White");
			ops.insertById(Person.class).one(person);
			throw new SimulateFailureException();
		}), TransactionFailedException.class, SimulateFailureException.class);

		Person fetched = ops.findById(Person.class).one(id.toString());
		assertNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a replace then rolling back")
	@Test
	public void rollbackReplace() {
		AtomicInteger attempts = new AtomicInteger();
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		ops.insertById(Person.class).one(person);

		assertThrowsWithCause(() -> doInTransaction(status -> {
			attempts.incrementAndGet();
			Person p = ops.findById(Person.class).one(person.getId().toString());
			p.setFirstname("changed");
			ops.replaceById(Person.class).one(p);
			throw new SimulateFailureException();
		}), TransactionFailedException.class, SimulateFailureException.class);

		Person fetched = ops.findById(Person.class).one(person.getId().toString());
		assertEquals("Walter", fetched.getFirstname());
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a remove then rolling back")
	@Test
	public void rollbackRemove() {
		AtomicInteger attempts = new AtomicInteger();
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		ops.insertById(Person.class).one(person);

		assertThrowsWithCause(() -> doInTransaction(status -> {
			attempts.incrementAndGet();
			Person p = ops.findById(Person.class).one(person.getId().toString());
			ops.removeById(Person.class).oneEntity(p);
			throw new SimulateFailureException();
		}), TransactionFailedException.class, SimulateFailureException.class);

		Person fetched = ops.findById(Person.class).one(person.getId().toString());
		assertNotNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a removeByQuery then rolling back")
	@Test
	public void rollbackRemoveByQuery() {
		AtomicInteger attempts = new AtomicInteger();
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		ops.insertById(Person.class).one(person);

		assertThrowsWithCause(() -> doInTransaction(status -> {
			attempts.incrementAndGet();
			ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq("Walter")).all();
			throw new SimulateFailureException();
		}), TransactionFailedException.class, SimulateFailureException.class);

		Person fetched = ops.findById(Person.class).one(person.getId().toString());
		assertNotNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a findByQuery then rolling back")
	@Test
	public void rollbackFindByQuery() {
		AtomicInteger attempts = new AtomicInteger();
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		ops.insertById(Person.class).one(person);

		assertThrowsWithCause(() -> doInTransaction(status -> {
			attempts.incrementAndGet();
			ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq("Walter")).all();
			throw new SimulateFailureException();
		}), TransactionFailedException.class, SimulateFailureException.class);

		assertEquals(1, attempts.get());
	}

	@DisplayName("Entity must have CAS field during replace")
	@Test
	public void replaceEntityWithoutCas() {
		UUID id = UUID.randomUUID();
		PersonWithoutVersion person = new PersonWithoutVersion(id, "Walter", "White");

		ops.insertById(PersonWithoutVersion.class).one(person);
		try {
			doInTransaction(status -> {
				PersonWithoutVersion fetched = ops.findById(PersonWithoutVersion.class).one(id.toString());
				ops.replaceById(PersonWithoutVersion.class).one(fetched);
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof IllegalArgumentException);
		}
	}

	@DisplayName("Entity must have non-zero CAS during replace")
	@Test
	public void replaceEntityWithCasZero() {
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		ops.insertById(Person.class).one(person);

		// switchedPerson here will have CAS=0, which will fail
		Person switchedPerson = new Person(id, "Dave", "Reynolds");

		assertThrowsWithCause(() -> doInTransaction(status -> {
			ops.replaceById(Person.class).one(switchedPerson);
		}), TransactionFailedException.class, IllegalArgumentException.class);
	}

	@DisplayName("Entity must have CAS field during remove")
	@Test
	public void removeEntityWithoutCas() {
		UUID id = UUID.randomUUID();
		PersonWithoutVersion person = new PersonWithoutVersion(id, "Walter", "White");

		ops.insertById(PersonWithoutVersion.class).one(person);
		assertThrowsWithCause(() -> doInTransaction(status -> {
			PersonWithoutVersion fetched = ops.findById(PersonWithoutVersion.class).one(id.toString());
			ops.removeById(PersonWithoutVersion.class).oneEntity(fetched);
		}), TransactionFailedException.class, IllegalArgumentException.class);
	}

	@DisplayName("removeById().one(id) isn't allowed in transactions, since we don't have the CAS")
	@Test
	public void removeEntityById() {
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		ops.insertById(Person.class).one(person);

		assertThrowsWithCause(() -> doInTransaction(status -> {
			Person p = ops.findById(Person.class).one(person.getId().toString());
			ops.removeById(Person.class).one(p.getId().toString());
		}), TransactionFailedException.class, IllegalArgumentException.class);
	}

	@DisplayName("setRollbackOnly should cause a rollback")
	@Test
	public void setRollbackOnly() {
		UUID id = UUID.randomUUID();

		assertThrowsWithCause(() -> doInTransaction(status -> {
			status.setRollbackOnly();
			Person person = new Person(id, "Walter", "White");
			ops.insertById(Person.class).one(person);
		}), TransactionFailedException.class);

		Person fetched = ops.findById(Person.class).one(id.toString());
		assertNull(fetched);
	}
}
