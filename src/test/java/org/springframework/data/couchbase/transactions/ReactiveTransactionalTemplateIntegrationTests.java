/*
 * Copyright 2022-2023 the original author or authors
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
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
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
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.TransactionalSupport;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.transaction.error.TransactionSystemUnambiguousException;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tests for reactive @Transactional, using the CouchbaseTransactionInterceptor.
 *
 * @author Graham Pople
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(
		classes = { TransactionsConfig.class, ReactiveTransactionalTemplateIntegrationTests.PersonService.class })
public class ReactiveTransactionalTemplateIntegrationTests extends JavaIntegrationTests {
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired PersonService personService;
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

	@DisplayName("A basic golden path insert should succeed")
	@Test
	public void committedInsert() {
		AtomicInteger tryCount = new AtomicInteger(0);

		personService.doInTransaction(tryCount, (ops) -> {
			return Mono.defer(() -> {
				return ops.insertById(Person.class).one(WalterWhite);
			});
		}).block();

		Person fetched = blocking.findById(Person.class).one(WalterWhite.id());
		assertEquals(WalterWhite.getFirstname(), fetched.getFirstname());
		assertEquals(1, tryCount.get());
	}

	@DisplayName("Basic test of doing an insert then rolling back")
	@Test
	public void rollbackInsert() {
		AtomicInteger tryCount = new AtomicInteger();

		assertThrowsWithCause(() -> {
			personService.doInTransaction(tryCount, (ops) -> {
				return Mono.defer(() -> {
					return ops.insertById(Person.class).one(WalterWhite).then(Mono.error(new SimulateFailureException()));
				});
			}).block();
		}, TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		Person fetched = blocking.findById(Person.class).one(WalterWhite.id());
		assertNull(fetched);
		assertEquals(1, tryCount.get());
	}

	@DisplayName("Forcing CAS mismatch causes a transaction retry")
	@Test
	public void casMismatchCausesRetry() {
		Person person = blocking.insertById(Person.class).one(WalterWhite);
		AtomicInteger attempts = new AtomicInteger();

		personService.doInTransaction(attempts, ops -> {
			return ops.findById(Person.class).one(person.id()).flatMap(fetched -> Mono.fromRunnable(() -> {
				ReplaceLoopThread.updateOutOfTransaction(blocking, person.withFirstName("ChangedExternally"), attempts.get());
			}).then(ops.replaceById(Person.class).one(fetched.withFirstName("Changed by transaction"))));
		}).block();

		Person fetched = blocking.findById(Person.class).one(person.getId().toString());
		assertEquals("Changed by transaction", fetched.getFirstname());
		assertEquals(2, attempts.get());
	}

	@Test
	public void returnMono() {
		AtomicInteger tryCount = new AtomicInteger(0);

		Person fromLambda = personService.doInTransactionReturningMono(tryCount, (ops) -> {
			return Mono.defer(() -> {
				return ops.insertById(Person.class).one(WalterWhite).log("source");
			}).log("returnMono test");
		}).block();

		assertNotNull(fromLambda);
		assertEquals(WalterWhite.getFirstname(), fromLambda.getFirstname());
	}

	@Test
	public void returnFlux() {
		AtomicInteger tryCount = new AtomicInteger(0);

		List<Integer> fromLambda = personService.doInTransactionReturningFlux(tryCount, (ops) -> {
			return Flux.defer(() -> {
				return ops.insertById(Person.class).one(WalterWhite)
						.thenMany(Flux.fromIterable(Arrays.asList(1, 2, 3)).log("1"));
			});
		}).collectList().block();

		assertEquals(3, fromLambda.size());
	}

	@Service // this will work in the unit tests even without @Service because of explicit loading by @SpringJUnitConfig
	static class PersonService {
		final ReactiveCouchbaseOperations ops;

		public PersonService(ReactiveCouchbaseOperations ops) {
			this.ops = ops;
		}

		@Transactional
		public Mono<Void> doInTransaction(AtomicInteger tryCount, Function<ReactiveCouchbaseOperations, Mono<?>> callback) {
			return TransactionalSupport.checkForTransactionInThreadLocalStorage().flatMap(stat -> {
				assertTrue(stat.isPresent(), "Not in transaction");
				tryCount.incrementAndGet();
				return callback.apply(ops).then();
			});
		}

		@Transactional
		public <T> Mono<T> doInTransactionReturningMono(AtomicInteger tryCount,
				Function<ReactiveCouchbaseOperations, Mono<T>> callback) {
			return Mono.defer(() -> {
				tryCount.incrementAndGet();
				return callback.apply(ops);
			});
		}

		@Transactional
		public <T> Flux<T> doInTransactionReturningFlux(AtomicInteger tryCount,
				Function<ReactiveCouchbaseOperations, Flux<T>> callback) {
			return Flux.defer(() -> {
				tryCount.incrementAndGet();
				return callback.apply(ops);
			});
		}
	}
}
