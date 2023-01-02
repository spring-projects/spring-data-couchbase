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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.transaction.error.TransactionSystemUnambiguousException;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;

/**
 * Tests for @Transactional methods, where parameters/options are being set that aren't support in a transaction. These
 * will be rejected at runtime.
 *
 * @author Graham Pople
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(classes = { TransactionsConfig.class,
		CouchbaseTransactionalUnsettableParametersIntegrationTests.PersonService.class })
public class CouchbaseTransactionalUnsettableParametersIntegrationTests extends JavaIntegrationTests {

	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired PersonService personService;

	Person WalterWhite;

	@BeforeEach
	public void beforeEachTest() {
		WalterWhite = new Person("Walter", "White");
	}

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
	}

	void test(Consumer<CouchbaseOperations> r) {
		AtomicInteger tryCount = new AtomicInteger(0);

		assertThrowsWithCause(() -> {
			personService.doInTransaction(tryCount, (ops) -> {
				r.accept(ops);
				return null;
			});
		}, TransactionSystemUnambiguousException.class, IllegalArgumentException.class);

		assertEquals(1, tryCount.get());
	}

	@DisplayName("Using insertById().withDurability - the PersistTo overload - in a transaction is rejected at runtime")
	@Test
	public void insertWithDurability() {
		test((ops) -> {
			ops.insertById(Person.class).withDurability(PersistTo.ONE, ReplicateTo.ONE).one(WalterWhite);
		});
	}

	@DisplayName("Using insertById().withExpiry in a transaction is rejected at runtime")
	@Test
	public void insertWithExpiry() {
		test((ops) -> {
			ops.insertById(Person.class).withExpiry(Duration.ofSeconds(3)).one(WalterWhite);
		});
	}

	@DisplayName("Using insertById().withDurability(durabilityLevel) in a transaction is rejected at runtime")
	@Test
	public void insertWithDurability2() {
		test((ops) -> {
			ops.insertById(Person.class).withDurability(DurabilityLevel.MAJORITY).one(WalterWhite);
		});
	}

	@DisplayName("Using insertById().withOptions in a transaction is rejected at runtime")
	@Test
	public void insertWithOptions() {
		test((ops) -> {
			ops.insertById(Person.class).withOptions(InsertOptions.insertOptions()).one(WalterWhite);
		});
	}

	@DisplayName("Using replaceById().withDurability - the PersistTo overload - in a transaction is rejected at runtime")
	@Test
	public void replaceWithDurability() {
		test((ops) -> {
			ops.replaceById(Person.class).withDurability(PersistTo.ONE, ReplicateTo.ONE).one(WalterWhite);
		});
	}

	@DisplayName("Using replaceById().withExpiry in a transaction is rejected at runtime")
	@Test
	public void replaceWithExpiry() {
		test((ops) -> {
			ops.replaceById(Person.class).withExpiry(Duration.ofSeconds(3)).one(WalterWhite);
		});
	}

	@DisplayName("Using replaceById().withDurability(durabilityLevel) in a transaction is rejected at runtime")
	@Test
	public void replaceWithDurability2() {
		test((ops) -> {
			ops.replaceById(Person.class).withDurability(DurabilityLevel.MAJORITY).one(WalterWhite);
		});
	}

	@DisplayName("Using replaceById().withOptions in a transaction is rejected at runtime")
	@Test
	public void replaceWithOptions() {
		test((ops) -> {
			ops.replaceById(Person.class).withOptions(ReplaceOptions.replaceOptions()).one(WalterWhite);
		});
	}

	@DisplayName("Using removeById().withDurability - the PersistTo overload - in a transaction is rejected at runtime")
	@Test
	public void removeWithDurability() {
		test((ops) -> {
			ops.removeById(Person.class).withDurability(PersistTo.ONE, ReplicateTo.ONE).oneEntity(WalterWhite);
		});
	}

	@DisplayName("Using removeById().withDurability(durabilityLevel) in a transaction is rejected at runtime")
	@Test
	public void removeWithDurability2() {
		test((ops) -> {
			ops.removeById(Person.class).withDurability(DurabilityLevel.MAJORITY).oneEntity(WalterWhite);
		});
	}

	@DisplayName("Using removeById().withOptions in a transaction is rejected at runtime")
	@Test
	public void removeWithOptions() {
		test((ops) -> {
			ops.removeById(Person.class).withOptions(RemoveOptions.removeOptions()).oneEntity(WalterWhite);
		});
	}

	@DisplayName("Using findById().withExpiry in a transaction is rejected at runtime")
	@Test
	public void findWithExpiry() {
		test((ops) -> {
			ops.replaceById(Person.class).withExpiry(Duration.ofSeconds(3)).one(WalterWhite);
		});
	}

	@DisplayName("Using findById().project in a transaction is rejected at runtime")
	@Test
	public void findProject() {
		test((ops) -> {
			ops.findById(Person.class).project(new String[] { "someField" }).one(WalterWhite.id());
		});
	}

	@DisplayName("Using findById().withOptions in a transaction is rejected at runtime")
	@Test
	public void findWithOptions() {
		test((ops) -> {
			ops.findById(Person.class).withOptions(GetOptions.getOptions()).one(WalterWhite.id());
		});
	}

	@Service // this will work in the unit tests even without @Service because of explicit loading by @SpringJUnitConfig
	static class PersonService {
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
