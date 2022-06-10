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

import com.couchbase.client.core.error.transaction.AttemptExpiredException;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

	Person WalterWhite;

	@BeforeEach
	public void beforeEachTest() {
		WalterWhite = new Person("Walter", "White");
		assertNotInTransaction();
	}

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

	private RunResult doInTransaction(Consumer<SpringTransactionAttemptContext> lambda,
			@Nullable TransactionOptions options) {
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

		RunResult rr = doInTransaction(ctx -> {
			ops.insertById(Person.class).one(WalterWhite);
		});

		Person fetched = ops.findById(Person.class).one(WalterWhite.id());
		assertEquals("Walter", fetched.getFirstname());
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path replace should succeed")
	@Test
	public void committedReplace() {
		Person p = ops.insertById(Person.class).one(WalterWhite);

		RunResult rr = doInTransaction(ctx -> {
			Person person = ops.findById(Person.class).one(p.id());
			person.setFirstname("changed");
			ops.replaceById(Person.class).one(person);
		});

		Person fetched = ops.findById(Person.class).one(p.id());
		assertEquals("changed", fetched.getFirstname());
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path remove should succeed")
	@Test
	public void committedRemove() {
		Person person = ops.insertById(Person.class).one(WalterWhite);

		RunResult rr = doInTransaction(ctx -> {
			Person fetched = ops.findById(Person.class).one(person.id());
			ops.removeById(Person.class).oneEntity(fetched);
		});

		Person fetched = ops.findById(Person.class).one(person.id());
		assertNull(fetched);
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path removeByQuery should succeed")
	@Test
	public void committedRemoveByQuery() {
		Person person = ops.insertById(Person.class).one(WalterWhite.withIdFirstname());

		RunResult rr = doInTransaction(ctx -> {
			List<RemoveResult> removed = ops.removeByQuery(Person.class)
					.matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all();
			assertEquals(1, removed.size());
		});

		Person fetched = ops.findById(Person.class).one(person.id());
		assertNull(fetched);
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path findByQuery should succeed")
	@Test
	public void committedFindByQuery() {
		Person person = ops.insertById(Person.class).one(WalterWhite.withIdFirstname());

		RunResult rr = doInTransaction(ctx -> {
			List<Person> found = ops.findByQuery(Person.class)
					.matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all();
			assertEquals(1, found.size());
		});

		assertEquals(1, rr.attempts);
	}

	@DisplayName("Basic test of doing an insert then rolling back")
	@Test
	public void rollbackInsert() {
		AtomicInteger attempts = new AtomicInteger();

		try {
			doInTransaction(ctx -> {
				attempts.incrementAndGet();
				Person person = ops.insertById(Person.class).one(WalterWhite);
				throw new SimulateFailureException();
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		Person fetched = ops.findById(Person.class).one(WalterWhite.id());
		assertNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a replace then rolling back")
	@Test
	public void rollbackReplace() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = ops.insertById(Person.class).one(WalterWhite);

		try {
			doInTransaction(ctx -> {
				attempts.incrementAndGet();
				Person p = ops.findById(Person.class).one(person.id());
				p.setFirstname("changed");
				ops.replaceById(Person.class).one(p);
				throw new SimulateFailureException();
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		Person fetched = ops.findById(Person.class).one(person.id());
		assertEquals("Walter", fetched.getFirstname());
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a remove then rolling back")
	@Test
	public void rollbackRemove() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = ops.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> {
			doInTransaction(ctx -> {
				attempts.incrementAndGet();
				Person p = ops.findById(Person.class).one(person.id());
				ops.removeById(Person.class).oneEntity(p);
				throw new SimulateFailureException();
			});
		}, TransactionFailedException.class, SimulateFailureException.class);

		Person fetched = ops.findById(Person.class).one(person.getId().toString());
		assertNotNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a removeByQuery then rolling back")
	@Test
	public void rollbackRemoveByQuery() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = ops.insertById(Person.class).one(WalterWhite.withIdFirstname());

		assertThrowsWithCause(() -> {
			doInTransaction(ctx -> {
				attempts.incrementAndGet();
				ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all();
				throw new SimulateFailureException();
			});
		}, TransactionFailedException.class, SimulateFailureException.class);

		Person fetched = ops.findById(Person.class).one(person.id());
		assertNotNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a findByQuery then rolling back")
	@Test
	public void rollbackFindByQuery() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = ops.insertById(Person.class).one(WalterWhite.withIdFirstname());

		assertThrowsWithCause(() -> {
			doInTransaction(ctx -> {
				attempts.incrementAndGet();
				ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all();
				throw new SimulateFailureException();
			});
		}, TransactionFailedException.class, SimulateFailureException.class);

		assertEquals(1, attempts.get());
	}

	@DisplayName("Create a Person outside a @Transactional block, modify it, and then replace that person in the @Transactional.  The transaction will retry until timeout.")
	@Test
	public void replacePerson() {
		Person person = ops.insertById(Person.class).one(WalterWhite);

		Person refetched = ops.findById(Person.class).one(person.id());
		ops.replaceById(Person.class).one(refetched);

		assertNotEquals(person.getVersion(), refetched.getVersion());

		assertThrowsWithCause(() -> {
			doInTransaction(ctx -> {
				ops.replaceById(Person.class).one(person);
			}, TransactionOptions.transactionOptions().timeout(Duration.ofSeconds(2)));
		}, TransactionFailedException.class, AttemptExpiredException.class);

	}

	@DisplayName("Entity must have CAS field during replace")
	@Test
	public void replaceEntityWithoutCas() {
		UUID id = UUID.randomUUID();
		PersonWithoutVersion person = ops.insertById(PersonWithoutVersion.class)
				.one(new PersonWithoutVersion(id, "Walter", "White"));
		assertThrowsWithCause(() -> {
			doInTransaction(ctx -> {
				PersonWithoutVersion fetched = ops.findById(PersonWithoutVersion.class).one(id.toString());
				ops.replaceById(PersonWithoutVersion.class).one(fetched);
			});
		}, TransactionFailedException.class, IllegalArgumentException.class);
	}

	@DisplayName("Entity must have non-zero CAS during replace")
	@Test
	public void replaceEntityWithCasZero() {
		Person person = ops.insertById(Person.class).one(new Person("Walter", "White"));

		// switchedPerson here will have CAS=0, which will fail
		Person switchedPerson = new Person(person.getId(), "Dave", "Reynolds");

		assertThrowsWithCause(() -> {
			doInTransaction(ctx -> {
				ops.replaceById(Person.class).one(switchedPerson);
			});
		}, TransactionFailedException.class, IllegalArgumentException.class);
	}

	@DisplayName("Entity must have CAS field during remove")
	@Test
	public void removeEntityWithoutCas() {
		UUID id = UUID.randomUUID();
		PersonWithoutVersion person = ops.insertById(PersonWithoutVersion.class)
				.one(new PersonWithoutVersion(id, "Walter", "White"));
		assertThrowsWithCause(() -> {
			doInTransaction(ctx -> {
				PersonWithoutVersion fetched = ops.findById(PersonWithoutVersion.class).one(id.toString());
				ops.removeById(PersonWithoutVersion.class).oneEntity(fetched);
			});
		}, TransactionFailedException.class, IllegalArgumentException.class);
	}

	@DisplayName("removeById().one(id) isn't allowed in transactions, since we don't have the CAS")
	@Test
	public void removeEntityById() {
		Person person = ops.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> {
			doInTransaction(ctx -> {
				Person p = ops.findById(Person.class).one(person.id());
				ops.removeById(Person.class).one(p.getId().toString());
			});
		}, TransactionFailedException.class, IllegalArgumentException.class);
	}

	@DisplayName("Forcing CAS mismatch causes a transaction retry")
	@Test
	public void casMismatchCausesRetry() {
		Person person = ops.insertById(Person.class).one(WalterWhite);
		AtomicInteger attempts = new AtomicInteger();

		doInTransaction(ctx -> {
			Person fetched = ops.findById(Person.class).one(person.getId().toString());
			ReplaceLoopThread.updateOutOfTransaction(ops, person.withFirstName("Changed externally"),
					attempts.incrementAndGet());
			ops.replaceById(Person.class).one(fetched.withFirstName("Changed by transaction"));
		});

		Person fetched = ops.findById(Person.class).one(person.getId().toString());
		assertEquals("Changed by transaction", fetched.getFirstname());
		assertEquals(2, attempts.get());
	}
}
