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

import com.couchbase.client.java.transactions.error.TransactionFailedException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.data.couchbase.transaction.error.TransactionSystemUnambiguousException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for @Transactional methods, where operations that aren't supported in a transaction are being used.
 * They should be prevented at runtime.
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(classes = {TransactionsConfig.class, CouchbaseTransactionalNonAllowableOperationsIntegrationTests.PersonService.class})
public class CouchbaseTransactionalNonAllowableOperationsIntegrationTests extends JavaIntegrationTests {

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

	void test(Consumer<CouchbaseOperations> r) {
		AtomicInteger tryCount = new AtomicInteger(0);

assertThrowsWithCause( () -> {
			personService.doInTransaction(tryCount, (ops) -> {
				r.accept(ops);
				return null;
			});
		}, TransactionSystemUnambiguousException.class, IllegalArgumentException.class);

		assertEquals(1, tryCount.get());
	}

	@DisplayName("Using existsById() in a transaction is rejected at runtime")
	@Test
	public void existsById() {
		test((ops) -> {
			ops.existsById(Person.class).one(WalterWhite.id());
		});
	}

	@DisplayName("Using findByAnalytics() in a transaction is rejected at runtime")
	@Test
	public void findByAnalytics() {
		test((ops) -> {
			ops.findByAnalytics(Person.class).one();
		});
	}

	@DisplayName("Using findFromReplicasById() in a transaction is rejected at runtime")
	@Test
	public void findFromReplicasById() {
		test((ops) -> {
			ops.findFromReplicasById(Person.class).any(WalterWhite.id());
		});
	}

	@DisplayName("Using upsertById() in a transaction is rejected at runtime")
	@Test
	public void upsertById() {
		test((ops) -> {
			ops.upsertById(Person.class).one(WalterWhite);
		});
	}

	@Service
	@Component
	@EnableTransactionManagement
	static
	class PersonService {
		final CouchbaseOperations personOperations;

		public PersonService(CouchbaseOperations ops) {
			personOperations = ops;
		}

		@Transactional
		public <T> T doInTransaction(AtomicInteger tryCount, Function<CouchbaseOperations, T> callback) {
			tryCount.incrementAndGet();
			return callback.apply(personOperations);
		}
	}
}
