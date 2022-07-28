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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.transaction.CouchbaseCallbackTransactionManager;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import org.springframework.data.couchbase.transaction.error.TransactionSystemUnambiguousException;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Tests for CouchbaseTransactionalOperator, using template methods (findById etc.)
 *
 * @author Graham Pople
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(TransactionsConfig.class)
public class CouchbaseTransactionalOperatorTemplateIntegrationTests extends JavaIntegrationTests {
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactiveCouchbaseTemplate ops;
	@Autowired CouchbaseTemplate blocking;

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

	private RunResult doMonoInTransaction(Supplier<Mono<?>> lambda) {
		CouchbaseCallbackTransactionManager manager = new CouchbaseCallbackTransactionManager(couchbaseClientFactory);
		TransactionalOperator operator = CouchbaseTransactionalOperator.create(manager);
		AtomicInteger attempts = new AtomicInteger();

		operator.transactional(Mono.fromRunnable(() -> attempts.incrementAndGet()).then(lambda.get())).block();

		assertNotInTransaction();

		return new RunResult(attempts.get());
	}

	@DisplayName("A basic golden path insert using CouchbaseSimpleTransactionalOperator.execute should succeed")
	@Test
	public void committedInsertWithExecute() {
		CouchbaseCallbackTransactionManager manager = new CouchbaseCallbackTransactionManager(couchbaseClientFactory);
		TransactionalOperator operator = CouchbaseTransactionalOperator.create(manager);

		operator.execute(v -> {
			return Mono.defer(() -> {
				return ops.insertById(Person.class).one(WalterWhite);
			});
		}).blockLast();

		Person fetched = blocking.findById(Person.class).one(WalterWhite.id());
		assertEquals(WalterWhite.getFirstname(), fetched.getFirstname());
	}

	@DisplayName("A basic golden path insert using CouchbaseSimpleTransactionalOperator.transactional(Flux) should succeed")
	@Test
	public void committedInsertWithFlux() {
		CouchbaseCallbackTransactionManager manager = new CouchbaseCallbackTransactionManager(couchbaseClientFactory);
		TransactionalOperator operator = CouchbaseTransactionalOperator.create(manager);

		Flux<Person> flux = Flux.defer(() -> {
			return ops.insertById(Person.class).one(WalterWhite);
		});

		operator.transactional(flux).blockLast();

		Person fetched = blocking.findById(Person.class).one(WalterWhite.id());
		assertEquals(WalterWhite.getFirstname(), fetched.getFirstname());
	}

	@DisplayName("A basic golden path insert using CouchbaseSimpleTransactionalOperator.transactional(Mono) should succeed")
	@Test
	public void committedInsert() {

		RunResult rr = doMonoInTransaction(() -> {
			return Mono.defer(() -> {
				return ops.insertById(Person.class).one(WalterWhite);
			});
		});

		Person fetched = blocking.findById(Person.class).one(WalterWhite.id());
		assertEquals(WalterWhite.getFirstname(), fetched.getFirstname());
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path replace should succeed")
	@Test
	public void committedReplace() {
		Person p = blocking.insertById(Person.class).one(WalterWhite);

		RunResult rr = doMonoInTransaction(() -> {
			return ops.findById(Person.class).one(WalterWhite.id()).flatMap(person -> {
				person.setFirstname("changed");
				return ops.replaceById(Person.class).one(person);
			});
		});

		Person fetched = blocking.findById(Person.class).one(WalterWhite.id());
		assertEquals("changed", fetched.getFirstname());
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path remove should succeed")
	@Test
	public void committedRemove() {

		Person person = blocking.insertById(Person.class).one(WalterWhite);

		RunResult rr = doMonoInTransaction(() -> {
			return ops.findById(Person.class).one(person.id())
					.flatMap(fetched -> ops.removeById(Person.class).oneEntity(fetched));
		});

		Person fetched = blocking.findById(Person.class).one(person.id());
		assertNull(fetched);
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path removeByQuery should succeed")
	@Test
	public void committedRemoveByQuery() {
		Person person = blocking.insertById(Person.class).one(WalterWhite.withIdFirstname());

		RunResult rr = doMonoInTransaction(() -> {
			return ops.removeByQuery(Person.class).withConsistency(REQUEST_PLUS)
					.matching(QueryCriteria.where("firstname").eq(person.id())).all().next();
		});

		Person fetched = blocking.findById(Person.class).one(person.id());
		assertNull(fetched);
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path findByQuery should succeed")
	@Test
	public void committedFindByQuery() {
		Person person = blocking.insertById(Person.class).one(WalterWhite.withIdFirstname());

		RunResult rr = doMonoInTransaction(() -> {
			return ops.findByQuery(Person.class).withConsistency(REQUEST_PLUS)
					.matching(QueryCriteria.where("firstname").eq(person.id())).all().next();
		});

		assertEquals(1, rr.attempts);
	}

	@DisplayName("Basic test of doing an insert then rolling back")
	@Test
	public void rollbackInsert() {
		AtomicInteger attempts = new AtomicInteger();

		assertThrowsWithCause(() -> doMonoInTransaction(() -> {
			attempts.incrementAndGet();
			return ops.insertById(Person.class).one(WalterWhite).map((p) -> throwSimulateFailureException(p));
		}), TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = blocking.findById(Person.class).one(WalterWhite.toString());
		assertNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a replace then rolling back")
	@Test
	public void rollbackReplace() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = blocking.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> doMonoInTransaction(() -> {
			attempts.incrementAndGet();
			return ops.findById(Person.class).one(person.id()) //
					.flatMap(p -> ops.replaceById(Person.class).one(p.withFirstName("changed"))) //
					.map(p -> throwSimulateFailureException(p));
		}), TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = blocking.findById(Person.class).one(person.id());
		assertEquals(person.getFirstname(), fetched.getFirstname());
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a remove then rolling back")
	@Test
	public void rollbackRemove() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = blocking.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> doMonoInTransaction(() -> {
			attempts.incrementAndGet();
			return ops.findById(Person.class).one(person.id()).flatMap(p -> ops.removeById(Person.class).oneEntity(p)) //
					.doOnSuccess(p -> throwSimulateFailureException(p)); // remove has no result
		}), TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = blocking.findById(Person.class).one(person.id());
		assertNotNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a removeByQuery then rolling back")
	@Test
	public void rollbackRemoveByQuery() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = blocking.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> doMonoInTransaction(() -> {
			attempts.incrementAndGet();
			return ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all()
					.elementAt(0).map(p -> throwSimulateFailureException(p));
		}), TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = blocking.findById(Person.class).one(person.id());
		assertNotNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a findByQuery then rolling back")
	@Test
	public void rollbackFindByQuery() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = blocking.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> doMonoInTransaction(() -> {
			attempts.incrementAndGet();
			return ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all()
					.elementAt(0).map(p -> throwSimulateFailureException(p));
		}), TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		assertEquals(1, attempts.get());
	}

	@DisplayName("Forcing CAS mismatch causes a transaction retry")
	@Test
	public void casMismatchCausesRetry() {
		Person person = blocking.insertById(Person.class).one(WalterWhite);
		AtomicInteger attempts = new AtomicInteger();

		// Needs to take place in a separate thread to bypass the ThreadLocalStorage checks
		Thread forceCASMismatch = new Thread(() -> {
			Person fetched = blocking.findById(Person.class).one(person.id());
			blocking.replaceById(Person.class).one(fetched.withFirstName("Changed externally"));
		});

		doMonoInTransaction(() -> {
			return ops.findById(Person.class).one(person.id()).flatMap(fetched -> Mono.defer(() -> {

				if (attempts.incrementAndGet() == 1) {
					forceCASMismatch.start();
					try {
						forceCASMismatch.join();
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
				return ops.replaceById(Person.class).one(fetched.withFirstName("Changed by transaction"));
			}));
		});

		Person fetched = blocking.findById(Person.class).one(person.id());
		assertEquals("Changed by transaction", fetched.getFirstname());
		assertEquals(2, attempts.get());
	}
}
