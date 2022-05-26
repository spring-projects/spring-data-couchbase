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

import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;
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
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

// todo gpx bad unsupported operations too

/**
 * Tests for @Transactional methods, where parameters/options are being set that aren't support in a transaction.
 * These will be rejected at runtime.
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(Config.class)
public class CouchbaseTransactionalUnsettableParametersIntegrationTests extends JavaIntegrationTests {

	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	PersonService personService;
	static GenericApplicationContext context;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
		context = new AnnotationConfigApplicationContext(Config.class, PersonService.class);
	}

	@BeforeEach
	public void beforeEachTest() {
		personService = context.getBean(PersonService.class); // getting it via autowired results in no @Transactional

		Person walterWhite = new Person(1, "Walter", "White");
		try {
			couchbaseClientFactory.getBucket().defaultCollection().remove(walterWhite.getId().toString());
		} catch (Exception ex) {
			// System.err.println(ex);
		}
	}

	void test(Consumer<CouchbaseOperations> r) {
		AtomicInteger tryCount = new AtomicInteger(0);

		try {
			personService.doInTransaction(tryCount, (ops) -> {
				r.accept(ops);
				return null;
			});
			fail("Transaction should not succeed");
		}
		catch (TransactionFailedException err) {
		}

		assertEquals(1, tryCount.get());
	}

	@DisplayName("Using insertById().withDurability - the PersistTo overload - in a transaction is rejected at runtime")
	@Test
	public void insertWithDurability() {
		test((ops) -> {
			Person person = new Person(1, "Walter", "White");
			ops.insertById(Person.class).withDurability(PersistTo.ONE, ReplicateTo.ONE).one(person);
		});
	}

	@DisplayName("Using insertById().withExpiry in a transaction is rejected at runtime")
	@Test
	public void insertWithExpiry() {
		test((ops) -> {
			Person person = new Person(1, "Walter", "White");
			ops.insertById(Person.class).withExpiry(Duration.ofSeconds(3)).one(person);
		});
	}

	@DisplayName("Using insertById().withOptions in a transaction is rejected at runtime")
	@Test
	public void insertWithOptions() {
		test((ops) -> {
			Person person = new Person(1, "Walter", "White");
			ops.insertById(Person.class).withOptions(InsertOptions.insertOptions()).one(person);
		});
	}

	@DisplayName("Using replaceById().withDurability - the PersistTo overload - in a transaction is rejected at runtime")
	@Test
	public void replaceWithDurability() {
		test((ops) -> {
			Person person = new Person(1, "Walter", "White");
			ops.replaceById(Person.class).withDurability(PersistTo.ONE, ReplicateTo.ONE).one(person);
		});
	}

	@DisplayName("Using replaceById().withExpiry in a transaction is rejected at runtime")
	@Test
	public void replaceWithExpiry() {
		test((ops) -> {
			Person person = new Person(1, "Walter", "White");
			ops.replaceById(Person.class).withExpiry(Duration.ofSeconds(3)).one(person);
		});
	}

	@DisplayName("Using replaceById().withOptions in a transaction is rejected at runtime")
	@Test
	public void replaceWithOptions() {
		test((ops) -> {
			Person person = new Person(1, "Walter", "White");
			ops.replaceById(Person.class).withOptions(ReplaceOptions.replaceOptions()).one(person);
		});
	}

	@DisplayName("Using removeById().withDurability - the PersistTo overload - in a transaction is rejected at runtime")
	@Test
	public void removeWithDurability() {
		test((ops) -> {
			Person person = new Person(1, "Walter", "White");
			ops.removeById(Person.class).withDurability(PersistTo.ONE, ReplicateTo.ONE).oneEntity(person);
		});
	}

	@DisplayName("Using removeById().withOptions in a transaction is rejected at runtime")
	@Test
	public void removeWithOptions() {
		test((ops) -> {
			Person person = new Person(1, "Walter", "White");
			ops.removeById(Person.class).withOptions(RemoveOptions.removeOptions()).oneEntity(person);
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

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public <T> T doInTransaction(AtomicInteger tryCount, Function<CouchbaseOperations, T> callback) {
			tryCount.incrementAndGet();
			return callback.apply(personOperations);
		}
	}
}
