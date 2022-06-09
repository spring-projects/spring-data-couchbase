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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertInTransaction;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonWithoutVersion;
import org.springframework.data.couchbase.transaction.SpringTransactionAttemptContext;
import org.springframework.data.couchbase.transaction.TransactionsWrapper;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.java.transactions.error.TransactionFailedException;
import reactor.util.annotation.Nullable;

/**
 * Tests for TransactionsWrapper, using template methods (findById etc.)
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(TransactionsConfigCouchbaseSimpleTransactionManager.class)
public class CouchbaseTransactionsWrapperTemplateIntegrationTests extends JavaIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired CouchbaseTemplate ops;

	@AfterEach
	public void afterEachTest() {
		assertNotInTransaction();
	}

	static class RunResult {
		public final TransactionResult result;
		public final int attempts;

		public RunResult(TransactionResult result, int attempts) {
			this.result = result;
			this.attempts = attempts;
		}
	}

	private RunResult doInTransaction(Consumer<SpringTransactionAttemptContext> lambda) {
		return doInTransaction(lambda, null);
	}

	private RunResult doInTransaction(Consumer<SpringTransactionAttemptContext> lambda, @Nullable TransactionOptions options) {
		TransactionsWrapper wrapper = new TransactionsWrapper(couchbaseClientFactory);
		AtomicInteger attempts = new AtomicInteger();

		TransactionResult result = wrapper.run(ctx -> {
			assertInTransaction();
			attempts.incrementAndGet();
			lambda.accept(ctx);
		}, options);

		assertNotInTransaction();

		return new RunResult(result, attempts.get());
	}

	@DisplayName("A basic golden path insert should succeed")
	@Test
	public void committedInsert() {
		UUID id = UUID.randomUUID();

		RunResult rr = doInTransaction(ctx -> {
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

		RunResult rr = doInTransaction(ctx -> {
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

		RunResult rr = doInTransaction(ctx -> {
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

		RunResult rr = doInTransaction(ctx -> {
			List<RemoveResult> removed = ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(id.toString())).all();
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

		RunResult rr = doInTransaction(ctx -> {
			List<Person> found = ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(id.toString())).all();
			assertEquals(1, found.size());
		});

		assertEquals(1, rr.attempts);
	}

	@DisplayName("Basic test of doing an insert then rolling back")
	@Test
	public void rollbackInsert() {
		AtomicInteger attempts = new AtomicInteger();
		UUID id = UUID.randomUUID();

		try {
			doInTransaction(ctx -> {
				attempts.incrementAndGet();
				Person person = new Person(id, "Walter", "White");
				ops.insertById(Person.class).one(person);
				throw new SimulateFailureException();
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

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

		try {
			doInTransaction(ctx -> {
				attempts.incrementAndGet();
				Person p = ops.findById(Person.class).one(person.getId().toString());
				p.setFirstname("changed");
				ops.replaceById(Person.class).one(p);
				throw new SimulateFailureException();
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

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

		try {
			doInTransaction(ctx -> {
				attempts.incrementAndGet();
				Person p = ops.findById(Person.class).one(person.getId().toString());
				ops.removeById(Person.class).oneEntity(p);
				throw new SimulateFailureException();
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

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

		try {
			doInTransaction(ctx -> {
				attempts.incrementAndGet();
				ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq("Walter")).all();
				throw new SimulateFailureException();
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

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

		try {
			doInTransaction(ctx -> {
				attempts.incrementAndGet();
				ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq("Walter")).all();
				throw new SimulateFailureException();
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		assertEquals(1, attempts.get());
	}


	@DisplayName("Create a Person outside a @Transactional block, modify it, and then replace that person in the @Transactional.  The transaction will retry until timeout.")
	@Test
	public void replacePerson() {
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		ops.insertById(Person.class).one(person);

		Person refetched = ops.findById(Person.class).one(person.getId().toString());
		ops.replaceById(Person.class).one(refetched);

		assertNotEquals(person.getVersion(), refetched.getVersion());

		try {
			doInTransaction(ctx -> {
				ops.replaceById(Person.class).one(person);
			}, TransactionOptions.transactionOptions().timeout(Duration.ofSeconds(2)));
			fail();
		} catch (TransactionFailedException ignored) {}

	}

	@DisplayName("Entity must have CAS field during replace")
	@Test
	public void replaceEntityWithoutCas() {
		UUID id = UUID.randomUUID();
		PersonWithoutVersion person = new PersonWithoutVersion(id, "Walter", "White");

		ops.insertById(PersonWithoutVersion.class).one(person);
		try {
			doInTransaction(ctx -> {
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

		try {
			doInTransaction(ctx -> {
				ops.replaceById(Person.class).one(switchedPerson);
			});

		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof IllegalArgumentException);
		}
	}

	@DisplayName("Entity must have CAS field during remove")
	@Test
	public void removeEntityWithoutCas() {
		UUID id = UUID.randomUUID();
		PersonWithoutVersion person = new PersonWithoutVersion(id, "Walter", "White");

		ops.insertById(PersonWithoutVersion.class).one(person);
		try {
			doInTransaction(ctx -> {
				PersonWithoutVersion fetched = ops.findById(PersonWithoutVersion.class).one(id.toString());
				ops.removeById(PersonWithoutVersion.class).oneEntity(fetched);
			});
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof IllegalArgumentException);
		}
	}

	@DisplayName("removeById().one(id) isn't allowed in transactions, since we don't have the CAS")
	@Test
	public void removeEntityById() {
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		ops.insertById(Person.class).one(person);

		try {
			doInTransaction(ctx -> {
				Person p = ops.findById(Person.class).one(person.getId().toString());
				ops.removeById(Person.class).one(p.getId().toString());
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof IllegalArgumentException);
		}
	}

	@DisplayName("Forcing CAS mismatch causes a transaction retry")
	@Test
	public void casMismatchCausesRetry() {
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		ops.insertById(Person.class).one(person);
		AtomicInteger attempts = new AtomicInteger();

		// Needs to take place in a separate thread to bypass the ThreadLocalStorage checks
		Thread forceCASMismatch = new Thread(() -> {
			Person fetched = ops.findById(Person.class).one(id.toString());
			ops.replaceById(Person.class).one(fetched.withFirstName("Changed externally"));
		});

		doInTransaction(ctx -> {
			Person fetched = ops.findById(Person.class).one(id.toString());

			if (attempts.incrementAndGet() == 1) {
				forceCASMismatch.start();
				try {
					forceCASMismatch.join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

			ops.replaceById(Person.class).one(fetched.withFirstName("Changed by transaction"));
		});

		Person fetched = ops.findById(Person.class).one(person.getId().toString());
		assertEquals("Changed by transaction", fetched.getFirstname());
		assertEquals(2, attempts.get());
	}
}
