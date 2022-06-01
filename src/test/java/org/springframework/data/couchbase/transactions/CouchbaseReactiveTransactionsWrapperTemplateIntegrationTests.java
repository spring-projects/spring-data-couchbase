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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.TransactionalSupport;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonWithoutVersion;
import org.springframework.data.couchbase.transaction.ReactiveTransactionsWrapper;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.error.TransactionFailedException;

/**
 * Tests for ReactiveTransactionsWrapper, using template methods (findById etc.)
 */
// todo gpx many of these tests are failing
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(Config.class)
public class CouchbaseReactiveTransactionsWrapperTemplateIntegrationTests extends JavaIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
	@Autowired ReactiveCouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactiveCouchbaseTemplate ops;
	@Autowired CouchbaseTemplate blocking;

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

	private RunResult doInTransaction(Function<ReactiveTransactionAttemptContext, Mono<?>> lambda) {
		return doInTransaction(lambda, null);
	}

	private RunResult doInTransaction(Function<ReactiveTransactionAttemptContext, Mono<?>> lambda,
			@Nullable TransactionOptions options) {
		ReactiveTransactionsWrapper wrapper = new ReactiveTransactionsWrapper(couchbaseClientFactory);
		AtomicInteger attempts = new AtomicInteger();

		TransactionResult result = wrapper.run(ctx -> {
			return TransactionalSupport.checkForTransactionInThreadLocalStorage(null).then(Mono.defer(() -> {
				attempts.incrementAndGet();
				return lambda.apply(ctx);
			}));
		}, options).block();

		assertNotInTransaction();

		return new RunResult(result, attempts.get());
	}

	@DisplayName("A basic golden path insert should succeed")
	@Test
	public void committedInsert() {
		UUID id = UUID.randomUUID();

		RunResult rr = doInTransaction(ctx -> {
			return Mono.defer(() -> {
				Person person = new Person(id, "Walter", "White");
				return ops.insertById(Person.class).one(person);
			});
		});

		Person fetched = blocking.findById(Person.class).one(id.toString());
		assertEquals("Walter", fetched.getFirstname());
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path replace should succeed")
	@Test
	public void committedReplace() {
		UUID id = UUID.randomUUID();
		Person initial = new Person(id, "Walter", "White");
		Person p = ops.insertById(Person.class).one(initial).block();

		RunResult rr = doInTransaction(ctx -> {
			return ops.findById(Person.class).one(id.toString()).flatMap(person -> {
				person.setFirstname("changed");
				return ops.replaceById(Person.class).one(person);
			});
		});

		Person fetched = blocking.findById(Person.class).one(initial.getId().toString());
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
			return ops.findById(Person.class).one(id.toString())
					.flatMap(fetched -> ops.removeById(Person.class).oneEntity(fetched));
		});

		Person fetched = blocking.findById(Person.class).one(person.getId().toString());
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
			return ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(id.toString())).all().then();
		});

		Person fetched = blocking.findById(Person.class).one(person.getId().toString());
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
			return ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(id.toString())).all().then();
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

		Person fetched = blocking.findById(Person.class).one(id.toString());
		assertNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a replace then rolling back")
	@Test
	public void rollbackReplace() {
		AtomicInteger attempts = new AtomicInteger();
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		Person insertedPerson = blocking.insertById(Person.class).one(person);

		try {
			doInTransaction(ctx -> {
				return Mono.defer(() -> {
					attempts.incrementAndGet();
					return ops.findById(Person.class).one(person.getId().toString()).flatMap(p -> {
						p.setFirstname("changed");
						return ops.replaceById(Person.class).one(p);
					}).then(Mono.error(new SimulateFailureException()));
				});
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		Person fetched = blocking.findById(Person.class).one(person.getId().toString());
		assertEquals("Walter", fetched.getFirstname());
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a remove then rolling back")
	@Test
	public void rollbackRemove() {
		AtomicInteger attempts = new AtomicInteger();
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		Person insertedPerson = blocking.insertById(Person.class).one(person);

		try {
			doInTransaction(ctx -> {
				return Mono.defer(() -> {
					attempts.incrementAndGet();
					return ops.findById(Person.class).one(person.getId().toString())
							// todo gpx failing because no next - seems to come from ctx.get itself
							.doOnNext(v -> System.out.println("next")).doFinally(v -> System.out.println("finally")).flatMap(p -> {
								return ops.removeById(Person.class).oneEntity(p);
							}).then(Mono.error(new SimulateFailureException()));
				});
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		Person fetched = blocking.findById(Person.class).one(person.getId().toString());
		assertNotNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a removeByQuery then rolling back")
	@Test
	public void rollbackRemoveByQuery() {
		AtomicInteger attempts = new AtomicInteger();
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		Person insertedPerson = blocking.insertById(Person.class).one(person);

		try {
			doInTransaction(ctx -> {
				return Mono.defer(() -> {
					attempts.incrementAndGet();
					return ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq("Walter")).all().then();
				}).then(Mono.error(new SimulateFailureException()));
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof SimulateFailureException);
		}

		Person fetched = blocking.findById(Person.class).one(person.getId().toString());
		assertNotNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a findByQuery then rolling back")
	@Test
	public void rollbackFindByQuery() {
		AtomicInteger attempts = new AtomicInteger();
		UUID id = UUID.randomUUID();
		Person person = new Person(id, "Walter", "White");
		Person insertedPerson = blocking.insertById(Person.class).one(person);

		try {
			doInTransaction(ctx -> {
				return Mono.defer(() -> {
					attempts.incrementAndGet();
					return ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq("Walter")).all().then();
				}).then(Mono.error(new SimulateFailureException()));
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
		Person insertedPerson = blocking.insertById(Person.class).one(person);

		Person refetched = blocking.findById(Person.class).one(person.getId().toString());
		blocking.replaceById(Person.class).one(refetched);

		assertNotEquals(person.getVersion(), refetched.getVersion());

		try {
			doInTransaction(ctx -> {
				return ops.replaceById(Person.class).one(person);
			}, TransactionOptions.transactionOptions().timeout(Duration.ofSeconds(2)));
			fail();
		} catch (TransactionFailedException ignored) {}

	}

	@DisplayName("Entity must have CAS field during replace")
	@Test
	public void replaceEntityWithoutCas() {
		UUID id = UUID.randomUUID();
		PersonWithoutVersion person = new PersonWithoutVersion(id, "Walter", "White");

		PersonWithoutVersion p = blocking.insertById(PersonWithoutVersion.class).one(person);
		try {
			doInTransaction(ctx -> {
				return ops.findById(PersonWithoutVersion.class).one(id.toString())
						.flatMap(fetched -> ops.replaceById(PersonWithoutVersion.class).one(fetched));
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
				return ops.replaceById(Person.class).one(switchedPerson);
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
				return ops.findById(PersonWithoutVersion.class).one(id.toString())
						.flatMap(fetched -> ops.removeById(PersonWithoutVersion.class).oneEntity(fetched));
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
		Person insertedPerson = blocking.insertById(Person.class).one(person);

		try {
			doInTransaction(ctx -> {
				return ops.findById(Person.class).one(person.getId().toString())
						.flatMap(p -> ops.removeById(Person.class).one(p.getId().toString()));
			});
			fail();
		} catch (TransactionFailedException err) {
			assertTrue(err.getCause() instanceof IllegalArgumentException);
		}
	}
}
