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
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.TransactionalSupport;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonWithoutVersion;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.error.TransactionFailedException;

/**
 * Tests using template methods (findById etc.) inside a regular reactive SDK transaction.
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(TransactionsConfig.class)
public class CouchbaseReactiveTransactionsWrapperTemplateIntegrationTests extends JavaIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactiveCouchbaseTemplate ops;
	@Autowired CouchbaseTemplate blocking;

	Person WalterWhite;

	@BeforeEach
	public void beforeEachTest(){
		WalterWhite = new Person ("Walter", "White");
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

	private RunResult doInTransaction(Function<ReactiveTransactionAttemptContext, Mono<?>> lambda) {
		return doInTransaction(lambda, null);
	}

	private RunResult doInTransaction(Function<ReactiveTransactionAttemptContext, Mono<?>> lambda,
			@Nullable TransactionOptions options) {
		AtomicInteger attempts = new AtomicInteger();

		TransactionResult result = couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> {
			return TransactionalSupport.checkForTransactionInThreadLocalStorage().then(Mono.defer(() -> {
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

		RunResult rr = doInTransaction(ctx -> {
			return Mono.defer(() -> {
				return ops.insertById(Person.class).one(WalterWhite);
			});
		});

		Person fetched = blocking.findById(Person.class).one(WalterWhite.id());
		assertEquals("Walter", fetched.getFirstname());
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path replace should succeed")
	@Test
	public void committedReplace() {

		Person initial = blocking.insertById(Person.class).one(WalterWhite);

		RunResult rr = doInTransaction(ctx -> {
			return ops.findById(Person.class).one(WalterWhite.id()).flatMap(person -> {
				person.setFirstname("changed");
				return ops.replaceById(Person.class).one(person);
			});
		});

		Person fetched = blocking.findById(Person.class).one(initial.id());
		assertEquals("changed", fetched.getFirstname());
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path remove should succeed")
	@Test
	public void committedRemove() {
		Person person = blocking.insertById(Person.class).one(WalterWhite);

		RunResult rr = doInTransaction(ctx -> {
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

		RunResult rr = doInTransaction(ctx -> {
			return ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(WalterWhite.id()))
					.withConsistency(QueryScanConsistency.REQUEST_PLUS).all().then();
		});

		Person fetched = blocking.findById(Person.class).one(person.id());
		assertNull(fetched);
		assertEquals(1, rr.attempts);
	}

	@DisplayName("A basic golden path findByQuery should succeed")
	@Test
	public void committedFindByQuery() {
		Person person = blocking.insertById(Person.class).one(WalterWhite.withIdFirstname());

		RunResult rr = doInTransaction(ctx -> {
			return ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(WalterWhite.getFirstname())).all().then();
		});

		assertEquals(1, rr.attempts);
	}

	@DisplayName("Basic test of doing an insert then rolling back")
	@Test
	public void rollbackInsert() {
		AtomicInteger attempts = new AtomicInteger();

		assertThrowsWithCause(() -> doInTransaction(ctx -> {
			attempts.incrementAndGet();
			return ops.insertById(Person.class).one(WalterWhite).map((p) -> throwSimulateFailureException(p));
		}), TransactionFailedException.class, SimulateFailureException.class);

		Person fetched = blocking.findById(Person.class).one(WalterWhite.id());
		assertNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a replace then rolling back")
	@Test
	public void rollbackReplace() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = blocking.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> doInTransaction(ctx -> {
			attempts.incrementAndGet();
			return ops.findById(Person.class).one(person.id()) //
					.flatMap(p -> ops.replaceById(Person.class).one(p.withFirstName("changed"))) //
					.map(p -> throwSimulateFailureException(p));
		}), TransactionFailedException.class, SimulateFailureException.class);

		Person fetched = blocking.findById(Person.class).one(person.id());
		assertEquals("Walter", fetched.getFirstname());
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a remove then rolling back")
	@Test
	public void rollbackRemove() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = blocking.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> doInTransaction(ctx -> {
			attempts.incrementAndGet();
			return ops.findById(Person.class).one(person.id())
					.flatMap(p -> ops.removeById(Person.class).oneEntity(p)) //
					.doOnSuccess(p -> throwSimulateFailureException(p)); // remove has no result
		}), TransactionFailedException.class, SimulateFailureException.class);

		Person fetched = blocking.findById(Person.class).one(person.id());
		assertNotNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a removeByQuery then rolling back")
	@Test
	public void rollbackRemoveByQuery() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = blocking.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> doInTransaction(ctx -> {
			attempts.incrementAndGet();
			return ops.removeByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all().elementAt(0)
					.map(p -> throwSimulateFailureException(p));
		}), TransactionFailedException.class, SimulateFailureException.class);

		Person fetched = blocking.findById(Person.class).one(person.id());
		assertNotNull(fetched);
		assertEquals(1, attempts.get());
	}

	@DisplayName("Basic test of doing a findByQuery then rolling back")
	@Test
	public void rollbackFindByQuery() {
		AtomicInteger attempts = new AtomicInteger();
		Person person = blocking.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> doInTransaction(ctx -> {
			attempts.incrementAndGet();
			return ops.findByQuery(Person.class).matching(QueryCriteria.where("firstname").eq(person.getFirstname())).all().elementAt(0)
					.map(p -> throwSimulateFailureException(p));
		}), TransactionFailedException.class, SimulateFailureException.class);

		assertEquals(1, attempts.get());
	}

	@DisplayName("Create a Person outside a @Transactional block, modify it, and then replace that person in the @Transactional.  The transaction will retry until timeout.")
	@Test
	public void replacePerson() {
		Person person = blocking.insertById(Person.class).one(WalterWhite);

		Person refetched = blocking.findById(Person.class).one(person.id());
		refetched = blocking.replaceById(Person.class).one(refetched); // new cas

		assertNotEquals(person.getVersion(), refetched.getVersion());

		assertThrowsWithCause(() -> doInTransaction(ctx -> ops.replaceById(Person.class).one(person), // old cas
				TransactionOptions.transactionOptions().timeout(Duration.ofSeconds(2))), TransactionFailedException.class ,
				Exception.class );

	}

	@DisplayName("Entity must have CAS field during replace")
	@Test
	public void replaceEntityWithoutCas() {
		;
		PersonWithoutVersion person = blocking.insertById(PersonWithoutVersion.class)
				.one(new PersonWithoutVersion("Walter", "White"));
		assertThrowsWithCause(
				() -> doInTransaction(ctx -> ops.findById(PersonWithoutVersion.class).one(person.id())
						.flatMap(fetched -> ops.replaceById(PersonWithoutVersion.class).one(fetched))),
				TransactionFailedException.class, IllegalArgumentException.class);

	}

	@DisplayName("Entity must have non-zero CAS during replace")
	@Test
	public void replaceEntityWithCasZero() {
		Person person = blocking.insertById(Person.class).one(WalterWhite);

		// switchedPerson here will have CAS=0, which will fail
		Person switchedPerson = new Person(person.getId(), "Dave", "Reynolds");

		assertThrowsWithCause(() -> doInTransaction(ctx -> {
			return ops.replaceById(Person.class).one(switchedPerson);
		}), TransactionFailedException.class, IllegalArgumentException.class);

	}

	@DisplayName("Entity must have CAS field during remove")
	@Test
	public void removeEntityWithoutCas() {
		PersonWithoutVersion person = blocking.insertById(PersonWithoutVersion.class).one(new PersonWithoutVersion("Walter", "White"));
		assertThrowsWithCause(() -> doInTransaction(ctx -> {
			return ops.findById(PersonWithoutVersion.class).one(person.id())
					.flatMap(fetched -> ops.removeById(PersonWithoutVersion.class).oneEntity(fetched));
		}), TransactionFailedException.class, IllegalArgumentException.class);

	}

	@DisplayName("removeById().one(id) isn't allowed in transactions, since we don't have the CAS")
	@Test
	public void removeEntityById() {
		Person person = blocking.insertById(Person.class).one(WalterWhite);

		assertThrowsWithCause(() -> doInTransaction(ctx -> {
			return ops.findById(Person.class).one(person.id())
					.flatMap(p -> ops.removeById(Person.class).one(p.id()));
		}), TransactionFailedException.class, IllegalArgumentException.class);

	}

	@DisplayName("Forcing CAS mismatch causes a transaction retry")
	@Test
	public void casMismatchCausesRetry() {
		Person person = blocking.insertById(Person.class).one(WalterWhite);
		AtomicInteger attempts = new AtomicInteger();

		doInTransaction(ctx -> {
			return ops.findById(Person.class).one(person.id()).flatMap(fetched -> Mono.defer(() -> {
				ReplaceLoopThread.updateOutOfTransaction(blocking, person.withFirstName("Changed externally"),
						attempts.incrementAndGet());
				return ops.replaceById(Person.class).one(fetched.withFirstName("Changed by transaction"));
			}));
		});

		Person fetched = blocking.findById(Person.class).one(person.id());
		assertEquals("Changed by transaction", fetched.getFirstname());
		assertEquals(2, attempts.get());
	}
}
