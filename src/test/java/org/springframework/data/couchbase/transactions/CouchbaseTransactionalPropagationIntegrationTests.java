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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.couchbase.client.core.error.transaction.RetryTransactionException;
import com.couchbase.client.java.transactions.error.TransactionFailedException;

/**
 * Tests for the various propagation values allowed on @Transactional methods.
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(classes = { TransactionsConfig.class,
		CouchbaseTransactionalPropagationIntegrationTests.PersonService.class })
public class CouchbaseTransactionalPropagationIntegrationTests extends JavaIntegrationTests {
	private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTransactionalPropagationIntegrationTests.class);

	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired PersonService personService;
	@Autowired CouchbaseTemplate operations;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
	}

	@BeforeEach
	public void beforeEachTest() {
		TransactionTestUtil.assertNotInTransaction();
	}

	@AfterEach
	public void afterEachTest() {
		assertNotInTransaction();
	}

	@DisplayName("Call @Transactional(propagation = DEFAULT) - succeeds, creates a transaction")
	@Test
	public void callDefault() {
		personService.propagationDefault(ops -> {
			assertInTransaction();
		});
	}

	@DisplayName("Call @Transactional(propagation = SUPPORTS) - fails as unsupported")
	@Test
	public void callSupports() {
		assertThrowsWithCause(() -> personService.propagationSupports(ops -> {}), UnsupportedOperationException.class);
	}

	@DisplayName("Call @Transactional(propagation = MANDATORY) - fails as not in an active transaction")
	@Test
	public void callMandatory() {
		assertThrowsWithCause(() -> personService.propagationMandatory(ops -> {}), IllegalTransactionStateException.class);
	}

	@DisplayName("Call @Transactional(propagation = REQUIRES_NEW) - fails as unsupported")
	@Test
	public void callRequiresNew() {
		assertThrowsWithCause(() -> personService.propagationRequiresNew(ops -> {}), UnsupportedOperationException.class);

	}

	@DisplayName("Call @Transactional(propagation = NOT_SUPPORTED) - fails as unsupported")
	@Test
	public void callNotSupported() {
		assertThrowsWithCause(() -> personService.propagationNotSupported(ops -> {}), UnsupportedOperationException.class);
	}

	@DisplayName("Call @Transactional(propagation = NEVER) - succeeds as not in a transaction, starts one")
	@Test
	public void callNever() {
		personService.propagationNever(ops -> {
			assertInTransaction();
		});
	}

	@DisplayName("Call @Transactional(propagation = NESTED) - succeeds as not in an existing transaction, starts one")
	@Test
	public void callNested() {
		personService.propagationNested(ops -> {
			assertInTransaction();
		});
	}

	@DisplayName("Call @Transactional that calls @Transactional(propagation = DEFAULT) - succeeds, continues existing")
	@Test
	public void callDefaultThatCallsDefault() {
		UUID id1 = UUID.randomUUID();
		UUID id2 = UUID.randomUUID();

		personService.propagationDefault(ops -> {
			ops.insertById(Person.class).one(new Person(id1, "Ada", "Lovelace"));

			personService.propagationDefault(ops2 -> {
				ops2.insertById(Person.class).one(new Person(id2, "Grace", "Hopper"));

				assertInTransaction();
			});
		});

		// Validate everything committed

		assertNotNull(operations.findById(Person.class).one(id1.toString()));
		assertNotNull(operations.findById(Person.class).one(id2.toString()));
	}

	@DisplayName("Call @Transactional that calls @Transactional(propagation = REQUIRED) - succeeds, continues existing")
	@Test
	public void callDefaultThatCallsRequired() {
		UUID id1 = UUID.randomUUID();
		UUID id2 = UUID.randomUUID();

		personService.propagationDefault(ops -> {
			ops.insertById(Person.class).one(new Person(id1, "Ada", "Lovelace"));

			personService.propagationRequired(ops2 -> {
				ops2.insertById(Person.class).one(new Person(id2, "Grace", "Hopper"));

				assertInTransaction();
			});
		});

		// Validate everything committed
		assertNotNull(operations.findById(Person.class).one(id1.toString()));
		assertNotNull(operations.findById(Person.class).one(id2.toString()));
	}

	@DisplayName("Call @Transactional that calls @Transactional(propagation = MANDATORY) - succeeds, continues existing")
	@Test
	public void callDefaultThatCallsMandatory() {
		UUID id1 = UUID.randomUUID();
		UUID id2 = UUID.randomUUID();

		personService.propagationDefault(ops -> {
			ops.insertById(Person.class).one(new Person(id1, "Ada", "Lovelace"));

			personService.propagationMandatory(ops2 -> {
				ops2.insertById(Person.class).one(new Person(id2, "Grace", "Hopper"));

				assertInTransaction();
			});
		});

		// Validate everything committed
		assertNotNull(operations.findById(Person.class).one(id1.toString()));
		assertNotNull(operations.findById(Person.class).one(id2.toString()));
	}

	@DisplayName("Call @Transactional that calls @Transactional(propagation = REQUIRES_NEW) - fails as unsupported")
	@Test
	public void callDefaultThatCallsRequiresNew() {
		UUID id1 = UUID.randomUUID();

		assertThrowsWithCause(() -> personService.propagationDefault(ops -> {
			ops.insertById(Person.class).one(new Person(id1, "Ada", "Lovelace"));
			personService.propagationRequiresNew(ops2 -> {});
		}), TransactionFailedException.class, UnsupportedOperationException.class);

		// Validate everything rolled back
		assertNull(operations.findById(Person.class).one(id1.toString()));
	}

	@DisplayName("Call @Transactional that calls @Transactional(propagation = NOT_SUPPORTED) - fails as unsupported")
	@Test
	public void callDefaultThatCallsNotSupported() {
		UUID id1 = UUID.randomUUID();

		assertThrowsWithCause(() -> {
			personService.propagationDefault(ops -> {
				ops.insertById(Person.class).one(new Person(id1, "Ada", "Lovelace"));
				personService.propagationNotSupported(ops2 -> {});
			});
		}, TransactionFailedException.class, UnsupportedOperationException.class);

		// Validate everything rolled back
		assertNull(operations.findById(Person.class).one(id1.toString()));
	}

	@DisplayName("Call @Transactional that calls @Transactional(propagation = NEVER) - fails as in a transaction")
	@Test
	public void callDefaultThatCallsNever() {
		UUID id1 = UUID.randomUUID();

		assertThrowsWithCause(() -> {
			personService.propagationDefault(ops -> {
				ops.insertById(Person.class).one(new Person(id1, "Ada", "Lovelace"));
				personService.propagationNever(ops2 -> {});
			});
		}, TransactionFailedException.class, IllegalTransactionStateException.class);

		// Validate everything rolled back
		assertNull(operations.findById(Person.class).one(id1.toString()));
	}

	@DisplayName("Call @Transactional that calls @Transactional(propagation = NESTED) - fails as unsupported")
	@Test
	public void callDefaultThatCallsNested() {
		UUID id1 = UUID.randomUUID();

		assertThrowsWithCause(() -> {
			personService.propagationDefault(ops -> {
				ops.insertById(Person.class).one(new Person(id1, "Ada", "Lovelace"));

				personService.propagationNested(ops2 -> {});
			});
		}, TransactionFailedException.class, UnsupportedOperationException.class);

		// Validate everything rolled back
		assertNull(operations.findById(Person.class).one(id1.toString()));
	}

	@DisplayName("Call @Transactional that calls @Transactional(propagation = DEFAULT) - check retries act correct")
	@Test
	public void callDefaultThatCallsDefaultRetries() {
		UUID id1 = UUID.randomUUID();
		UUID id2 = UUID.randomUUID();
		AtomicInteger attempts = new AtomicInteger();

		personService.propagationDefault(ops -> {
			ops.insertById(Person.class).one(new Person(id1, "Ada", "Lovelace"));

			personService.propagationDefault(ops2 -> {
				ops2.insertById(Person.class).one(new Person(id2, "Grace", "Hopper"));
				assertInTransaction();

				if (attempts.incrementAndGet() < 3) {
					throw new RetryTransactionException();
				}
			});
		});

		// Validate everything committed
		assertNotNull(operations.findById(Person.class).one(id1.toString()));
		assertNotNull(operations.findById(Person.class).one(id2.toString()));
		assertEquals(3, attempts.get());
	}

	void assertInTransaction() {
		assertTrue(org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive());
	}

	void assertNotInTransaction() {
		try {
			assertFalse(
					org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive());
		} catch (NoTransactionException ignored) {}
	}

	@Service
	@Component
	@EnableTransactionManagement
	static class PersonService {
		final CouchbaseOperations ops;

		public PersonService(CouchbaseOperations ops) {
			this.ops = ops;
		}

		@Transactional
		public void propagationDefault(@Nullable Consumer<CouchbaseOperations> callback) {
			LOGGER.info("propagationDefault");
			if (callback != null)
				callback.accept(ops);
		}

		@Transactional(propagation = Propagation.REQUIRED)
		public void propagationRequired(@Nullable Consumer<CouchbaseOperations> callback) {
			LOGGER.info("propagationRequired");
			if (callback != null)
				callback.accept(ops);
		}

		@Transactional(propagation = Propagation.MANDATORY)
		public void propagationMandatory(@Nullable Consumer<CouchbaseOperations> callback) {
			LOGGER.info("propagationMandatory");
			if (callback != null)
				callback.accept(ops);
		}

		@Transactional(propagation = Propagation.NESTED)
		public void propagationNested(@Nullable Consumer<CouchbaseOperations> callback) {
			LOGGER.info("propagationNever");
			if (callback != null)
				callback.accept(ops);
		}

		@Transactional(propagation = Propagation.SUPPORTS)
		public void propagationSupports(@Nullable Consumer<CouchbaseOperations> callback) {
			LOGGER.info("propagationSupports");
			if (callback != null)
				callback.accept(ops);
		}

		@Transactional(propagation = Propagation.NOT_SUPPORTED)
		public void propagationNotSupported(@Nullable Consumer<CouchbaseOperations> callback) {
			LOGGER.info("propagationNotSupported");
			if (callback != null)
				callback.accept(ops);
		}

		@Transactional(propagation = Propagation.REQUIRES_NEW)
		public void propagationRequiresNew(@Nullable Consumer<CouchbaseOperations> callback) {
			LOGGER.info("propagationRequiresNew");
			if (callback != null)
				callback.accept(ops);
		}

		@Transactional(propagation = Propagation.NEVER)
		public void propagationNever(@Nullable Consumer<CouchbaseOperations> callback) {
			LOGGER.info("propagationNever");
			if (callback != null)
				callback.accept(ops);
		}
	}
}
