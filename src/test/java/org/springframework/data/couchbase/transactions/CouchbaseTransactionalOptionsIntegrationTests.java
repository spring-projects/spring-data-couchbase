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
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for @Transactional methods, setting all the various options allowed by @Transactional.
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(Config.class)
public class CouchbaseTransactionalOptionsIntegrationTests extends JavaIntegrationTests {

	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	PersonService personService;
	@Autowired
	CouchbaseTemplate operations;
	static GenericApplicationContext context;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
		context = new AnnotationConfigApplicationContext(Config.class, PersonService.class);
	}

	@BeforeEach
	public void beforeEachTest() {
		personService = context.getBean(PersonService.class);

		Person walterWhite = new Person(1, "Walter", "White");
		try {
			couchbaseClientFactory.getBucket().defaultCollection().remove(walterWhite.getId().toString());
		} catch (Exception ex) {
			// System.err.println(ex);
		}
	}

	@DisplayName("@Transactional(timeout = 2) will timeout at around 2 seconds")
	@Test
	public void timeout() {
		long start = System.nanoTime();
		Person person = new Person(1, "Walter", "White");
		operations.insertById(Person.class).one(person);
		try {
			personService.timeout(person.getId().toString());
			fail();
		}
		catch (TransactionFailedException err) {
		}
		Duration timeTaken = Duration.ofNanos(System.nanoTime() - start);
		assertTrue(timeTaken.toMillis() >= 2000);
		assertTrue(timeTaken.toMillis() < 10_000);  // Default transaction timeout is 15s
	}

	@DisplayName("@Transactional(isolation = Isolation.ANYTHING_BUT_READ_COMMITTED) will fail")
	@Test
	public void unsupportedIsolation() {
		try {
			personService.unsupportedIsolation();
			fail();
		}
		catch (IllegalArgumentException err) {
		}
	}

	@DisplayName("@Transactional(isolation = Isolation.READ_COMMITTED) will succeed")
	@Test
	public void supportedIsolation() {
		personService.supportedIsolation();
	}

	@Service
	@Component
	@EnableTransactionManagement
	static
	class PersonService {
		final CouchbaseOperations ops;

		public PersonService(CouchbaseOperations ops) {
			this.ops = ops;
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public <T> T doInTransaction(AtomicInteger tryCount, Function<CouchbaseOperations, T> callback) {
			tryCount.incrementAndGet();
			return callback.apply(ops);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER, timeout = 2)
		public void timeout(String id) {
			while (true) {
				Person p = ops.findById(Person.class).one(id);
				ops.replaceById(Person.class).one(p);
			}
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER, isolation = Isolation.REPEATABLE_READ)
		public void unsupportedIsolation() {
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER, isolation = Isolation.READ_COMMITTED)
		public void supportedIsolation() {
		}
	}
}
