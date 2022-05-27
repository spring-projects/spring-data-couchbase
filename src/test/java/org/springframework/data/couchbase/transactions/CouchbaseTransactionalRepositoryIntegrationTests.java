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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertInTransaction;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

/**
 * Tests @Transactional with repository methods.
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(Config.class)
public class CouchbaseTransactionalRepositoryIntegrationTests extends JavaIntegrationTests {
	@Autowired UserRepository userRepo;
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
	}

	@AfterEach
	public void afterEachTest() {
		assertNotInTransaction();
	}


	@Test
	public void findByFirstname() {
		operations.insertById(User.class).one(new User(UUID.randomUUID().toString(), "Ada", "Lovelace"));

		List<User> users = personService.findByFirstname("Ada");

		assertNotEquals(0, users.size());
	}

	@Test
	public void save() {
		String id = UUID.randomUUID().toString();

		personService.run(repo -> {
			assertInTransaction();

			repo.save(new User(id, "Ada", "Lovelace"));

			assertInTransaction();

			// read your own write
			// todo gpx this is failing because it's being executed non-transactionally, due to a bug somewhere
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
			personService.run(repo -> {
				repo.save(new User(id, "Ada", "Lovelace"));
				SimulateFailureException.throwEx("fail");
			});
			fail();
		}
		catch (TransactionFailedException ignored) {
		}

		User user = operations.findById(User.class).one(id);
		assertNull(user);
	}


	@Service
	@Component
	@EnableTransactionManagement
	static
	class PersonService {
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
