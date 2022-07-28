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

package org.springframework.data.couchbase.transactions.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;

import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.transactions.TransactionsConfig;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.java.transactions.error.TransactionFailedException;

/**
 * Tests for regular reactive SDK transactions, where Spring operations that aren't supported in a transaction are being
 * used. They should be prevented at runtime.
 *
 * @author Graham Pople
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(classes = { TransactionsConfig.class,
		SDKReactiveTransactionsNonAllowableOperationsIntegrationTests.PersonService.class })
public class SDKReactiveTransactionsNonAllowableOperationsIntegrationTests extends JavaIntegrationTests {

	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired PersonService personService;

	Person WalterWhite;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
	}

	@BeforeEach
	public void beforeEachTest() {
		WalterWhite = new Person("Walter", "White");
		TransactionTestUtil.assertNotInTransaction();
	}

	void test(Function<ReactiveCouchbaseOperations, Mono<?>> r) {
		AtomicInteger tryCount = new AtomicInteger(0);

		assertThrowsWithCause(() -> {
			couchbaseClientFactory.getCluster().reactive().transactions().run(ignored -> {
				return personService.doInService(tryCount, (ops) -> {
					return r.apply(ops);
				});
			}).block();
		}, TransactionFailedException.class, IllegalArgumentException.class);

		assertEquals(1, tryCount.get());
	}

	@DisplayName("Using existsById() in a transaction is rejected at runtime")
	@Test
	public void existsById() {
		test((ops) -> {
			return ops.existsById(Person.class).one(WalterWhite.id());
		});
	}

	@DisplayName("Using findByAnalytics() in a transaction is rejected at runtime")
	@Test
	public void findByAnalytics() {
		test((ops) -> {
			return ops.findByAnalytics(Person.class).one();
		});
	}

	@DisplayName("Using findFromReplicasById() in a transaction is rejected at runtime")
	@Test
	public void findFromReplicasById() {
		test((ops) -> {
			return ops.findFromReplicasById(Person.class).any(WalterWhite.id());
		});
	}

	@DisplayName("Using upsertById() in a transaction is rejected at runtime")
	@Test
	public void upsertById() {
		test((ops) -> {
			return ops.upsertById(Person.class).one(WalterWhite);
		});
	}

	// This is intentionally not a @Transactional service
	@Service // this will work in the unit tests even without @Service because of explicit loading by @SpringJUnitConfig
	static class PersonService {
		final ReactiveCouchbaseOperations personOperations;

		public PersonService(ReactiveCouchbaseOperations ops) {
			personOperations = ops;
		}

		public <T> Mono<T> doInService(AtomicInteger tryCount, Function<ReactiveCouchbaseOperations, Mono<T>> callback) {
			tryCount.incrementAndGet();
			return callback.apply(personOperations);
		}
	}
}
