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

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertInTransaction;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInReactiveTransaction;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import com.couchbase.client.java.Cluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import com.couchbase.client.java.transactions.error.TransactionFailedException;

/**
 * Tests @Transactional with repository methods.
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(classes = { TransactionsConfigCouchbaseSimpleTransactionManager.class, CouchbaseTransactionalRepositoryIntegrationTests.UserService.class })
public class CouchbaseTransactionalRepositoryIntegrationTests extends JavaIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
	@Autowired UserRepository userRepo;
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired
	UserService userService;
	@Autowired CouchbaseTemplate operations;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
	}

	@BeforeEach
	public void beforeEachTest() {
		assertNotInTransaction();
		assertNotInReactiveTransaction();
	}

	@AfterEach
	public void afterEachTest() {
		assertNotInTransaction();
		assertNotInReactiveTransaction();
	}

	@Test
	public void findByFirstname() {
		operations.insertById(User.class).one(new User(UUID.randomUUID().toString(), "Ada", "Lovelace"));

		List<User> users = userService.findByFirstname("Ada");

		assertNotEquals(0, users.size());
	}

	@Test
	public void save() {
		String id = UUID.randomUUID().toString();

		userService.run(repo -> {
			assertInTransaction();

			repo.save(new User(id, "Ada", "Lovelace"));

			assertInTransaction();

			// read your own write
			User user = operations.findById(User.class).one(id);
			assertNotNull(user);

			assertInTransaction();

		});

		User user = operations.findById(User.class).one(id);
		assertNotNull(user);
	}

	@DisplayName("Test that repo.save() is actually performed transactionally, by forcing a rollback")
	@Test
	public void saveRolledBack() {
		String id = UUID.randomUUID().toString();

		try {
			userService.run(repo -> {
				repo.save(new User(id, "Ada", "Lovelace"));
				SimulateFailureException.throwEx("fail");
			});
			fail();
		} catch (TransactionFailedException ignored) {}

		User user = operations.findById(User.class).one(id);
		assertNull(user);
	}

	@Service
	@Component
	@EnableTransactionManagement
	static class UserService {
		@Autowired UserRepository userRepo;

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public void run(Consumer<UserRepository> callback) {
			callback.accept(userRepo);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
		public List<User> findByFirstname(String name) {
			return userRepo.findByFirstname(name);
		}

	}


}
