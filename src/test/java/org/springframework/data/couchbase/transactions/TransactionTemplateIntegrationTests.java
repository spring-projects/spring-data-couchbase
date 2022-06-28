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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonWithoutVersion;
import org.springframework.data.couchbase.transaction.CouchbaseCallbackTransactionManager;
import org.springframework.data.couchbase.transaction.error.TransactionSystemUnambiguousException;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Tests for Spring's TransactionTemplate, used CouchbaseCallbackTransactionManager, using template methods (findById
 * etc.)
 *
 * @author Graham Pople
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(TransactionsConfig.class)
public class TransactionTemplateIntegrationTests extends JavaIntegrationTests {
	TransactionTemplate template;
	@Autowired CouchbaseCallbackTransactionManager transactionManager;
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired CouchbaseTemplate ops;
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
		assertNotInTransaction();
		List<RemoveResult> rp0 = ops.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<RemoveResult> rp1 = ops.removeByQuery(PersonWithoutVersion.class).withConsistency(REQUEST_PLUS).all();

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
		AtomicInteger tryCount = new AtomicInteger();

		template.executeWithoutResult(status -> {
			TransactionTestUtil.assertInTransaction();
			assertFalse(status.hasSavepoint());
			assertFalse(status.isRollbackOnly());
			assertFalse(status.isCompleted());
			assertTrue(status.isNewTransaction());

			tryCount.incrementAndGet();
			lambda.accept(status);
		});

		TransactionTestUtil.assertNotInTransaction();

		return new RunResult(tryCount.get());
	}

	@DisplayName("A basic golden path insert should succeed")
	@Test
	public void committedInsert() {
		RunResult rr = doInTransaction(status -> {
			ops.insertById(Person.class).one(WalterWhite);
		});

		Person fetched = ops.findById(Person.class).one(WalterWhite.id());
		assertEquals(WalterWhite.getFirstname(), fetched.getFirstname());
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path replace should succeed")
	@Test
	public void committedReplace() {
		Person person = ops.insertById(Person.class).one(WalterWhite);

		RunResult rr = doInTransaction(status -> {
			Person p = ops.findById(Person.class).one(person.id());
			ops.replaceById(Person.class).one(p.withFirstName("changed"));
		});

		Person fetched = ops.findById(Person.class).one(person.id());
		assertEquals("changed", fetched.getFirstname());
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path remove should succeed")
	@Test
	public void committedRemove() {
		Person person = ops.insertById(Person.class).one(WalterWhite);

		RunResult rr = doInTransaction(status -> {
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

		RunResult rr = doInTransaction(status -> {
			List<RemoveResult> removed = ops.removeByQuery(Person.class).withConsistency(REQUEST_PLUS)
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

		RunResult rr = doInTransaction(status -> {
			List<Person> found = ops.findByQuery(Person.class).withConsistency(REQUEST_PLUS)
					.matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all();
			assertEquals(1, found.size());
		});

		assertEquals(1, rr.attempts);
	}

	@DisplayName("Basic test of doing an insert then rolling back")
	@Test
	public void rollbackInsert() {
		AtomicInteger attempts = new AtomicInteger();

		assertThrowsWithCause(() -> doInTransaction(status -> {
			attempts.incrementAndGet();
			Person person = ops.insertById(Person.class).one(WalterWhite);
			throw new SimulateFailureException();
		}), TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = ops.findById(Person.class).one(WalterWhite.id());
		assertNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a replace then rolling back")
	@Test
	public void rollbackReplace() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = ops.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> doInTransaction(status -> {
			attempts.incrementAndGet();
			Person p = ops.findById(Person.class).one(person.id());
			p.setFirstname("changed");
			ops.replaceById(Person.class).one(p);
			throw new SimulateFailureException();
		}), TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = ops.findById(Person.class).one(person.id());
		assertEquals(person.getFirstname(), fetched.getFirstname());
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a remove then rolling back")
	@Test
	public void rollbackRemove() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = ops.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> doInTransaction(status -> {
			attempts.incrementAndGet();
			Person p = ops.findById(Person.class).one(person.id());
			ops.removeById(Person.class).oneEntity(p);
			throw new SimulateFailureException();
		}), TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = ops.findById(Person.class).one(person.id());
		assertNotNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a removeByQuery then rolling back")
	@Test
	public void rollbackRemoveByQuery() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = ops.insertById(Person.class).one(WalterWhite.withIdFirstname());

		assertThrowsWithCause(() -> doInTransaction(status -> {
			attempts.incrementAndGet();
			ops.removeByQuery(Person.class).withConsistency(REQUEST_PLUS)
					.matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all();
			throw new SimulateFailureException();
		}), TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = ops.findById(Person.class).one(person.id());
		assertNotNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a findByQuery then rolling back")
	@Test
	public void rollbackFindByQuery() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = ops.insertById(Person.class).one(WalterWhite.withIdFirstname());

		assertThrowsWithCause(() -> doInTransaction(status -> {
			attempts.incrementAndGet();
			ops.findByQuery(Person.class).withConsistency(REQUEST_PLUS)
					.matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all();
			throw new SimulateFailureException();
		}), TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		assertEquals(1, attempts.get());
	}

	@DisplayName("Entity must have CAS field during replace")
	@Test
	public void replaceEntityWithoutCas() {
		PersonWithoutVersion person = ops.insertById(PersonWithoutVersion.class)
				.one(new PersonWithoutVersion(UUID.randomUUID(), "Walter", "White"));
		assertThrowsWithCause(() -> {
			doInTransaction(status -> {
				PersonWithoutVersion fetched = ops.findById(PersonWithoutVersion.class).one(person.id());
				ops.replaceById(PersonWithoutVersion.class).one(fetched);
			});
		}, TransactionSystemUnambiguousException.class, IllegalArgumentException.class);

	}

	@DisplayName("Entity must have non-zero CAS during replace")
	@Test
	public void replaceEntityWithCasZero() {
		Person person = ops.insertById(Person.class).one(WalterWhite);

		// switchedPerson here will have CAS=0, which will fail
		Person switchedPerson = new Person(person.getId(), "Dave", "Reynolds");

		assertThrowsWithCause(() -> doInTransaction(status -> {
			ops.replaceById(Person.class).one(switchedPerson);
		}), TransactionSystemUnambiguousException.class, IllegalArgumentException.class);
	}

	@DisplayName("Entity must have CAS field during remove")
	@Test
	public void removeEntityWithoutCas() {
		PersonWithoutVersion person = ops.insertById(PersonWithoutVersion.class)
				.one(new PersonWithoutVersion(UUID.randomUUID(), "Walter", "White"));
		assertThrowsWithCause(() -> doInTransaction(status -> {
			PersonWithoutVersion fetched = ops.findById(PersonWithoutVersion.class).one(person.id());
			ops.removeById(PersonWithoutVersion.class).oneEntity(fetched);
		}), TransactionSystemUnambiguousException.class, IllegalArgumentException.class);
	}

	@DisplayName("removeById().one(id) isn't allowed in transactions, since we don't have the CAS")
	@Test
	public void removeEntityById() {
		Person person = ops.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> doInTransaction(status -> {
			Person p = ops.findById(Person.class).one(person.id());
			ops.removeById(Person.class).one(p.id());
		}), TransactionSystemUnambiguousException.class, IllegalArgumentException.class);
	}

	@DisplayName("setRollbackOnly should cause a rollback")
	@Test
	public void setRollbackOnly() {

		assertThrowsWithCause(() -> doInTransaction(status -> {
			status.setRollbackOnly();
			Person person = ops.insertById(Person.class).one(WalterWhite);
		}), TransactionSystemUnambiguousException.class);

		Person fetched = ops.findById(Person.class).one(WalterWhite.id());
		assertNull(fetched);
	}

	@DisplayName("Setting an unsupported isolation level should fail")
	@Test
	public void unsupportedIsolationLevel() {
		template.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);

		assertThrowsWithCause(() -> doInTransaction(status -> {}), IllegalArgumentException.class);
	}

	@DisplayName("Setting PROPAGATION_MANDATORY should fail, as not in a transaction")
	@Test
	public void propagationMandatoryOutsideTransaction() {
		template.setPropagationBehavior(TransactionDefinition.PROPAGATION_MANDATORY);

		assertThrowsWithCause(() -> doInTransaction(status -> {}), IllegalTransactionStateException.class);
	}

	@Test
	public void nestedTransactionTemplates() {
		TransactionTemplate template2 = new TransactionTemplate(transactionManager);
		template2.setPropagationBehavior(TransactionDefinition.PROPAGATION_MANDATORY);

		template.executeWithoutResult(status -> {
			template2.executeWithoutResult(status2 -> {
				Person person = ops.insertById(Person.class).one(WalterWhite);
			});
		});

		Person fetched = ops.findById(Person.class).one(WalterWhite.id());
		assertEquals("Walter", fetched.getFirstname());
	}

}
