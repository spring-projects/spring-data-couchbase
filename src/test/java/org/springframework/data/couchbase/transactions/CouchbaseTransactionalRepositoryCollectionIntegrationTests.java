/*
 * Copyright 2022-present the original author or authors
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
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertInTransaction;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.domain.UserCol;
import org.springframework.data.couchbase.domain.UserColRepository;
import org.springframework.data.couchbase.transaction.error.TransactionSystemUnambiguousException;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.CollectionAwareIntegrationTests;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.stereotype.Service;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tests @Transactional with repository methods.
 *
 * @author Michael Reiche
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(classes = { TransactionsConfig.class, CouchbaseTransactionalRepositoryCollectionIntegrationTests.UserService.class })
@DirtiesContext
public class CouchbaseTransactionalRepositoryCollectionIntegrationTests extends CollectionAwareIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
    @Autowired UserColRepository userRepo;
    @Autowired UserService userService;
	//@Autowired CouchbaseTemplate operations;

	@BeforeAll
	public static void beforeAll() {
        callSuperBeforeAll(new Object() {});
	}

	@BeforeEach
	public void beforeEachTest() {
		assertNotInTransaction();
	}

	@AfterEach
	public void afterEachTest() {
		assertNotInTransaction();
	}

	@Test
	public void findByFirstname() {
		userRepo.getOperations().insertById(UserCol.class).one(new UserCol(UUID.randomUUID().toString(), "Ada", "Lovelace"));

		List<UserCol> users = userService.findByFirstname("Ada");

		assertNotEquals(0, users.size());
	}

	@Test
	public void save() {
		String id = UUID.randomUUID().toString();

		userService.run(repo -> {
			assertInTransaction();

			UserCol user0 = repo.save(new UserCol(id, "Ada", "Lovelace"));

			assertInTransaction();

			// read your own write
			UserCol user1 = userRepo.getOperations().findById(UserCol.class).one(id);
			assertNotNull(user1);

			assertInTransaction();

		});

		UserCol user = userRepo.getOperations().findById(UserCol.class).one(id);
		assertNotNull(user);
	}

	@DisplayName("Test that repo.save() is actually performed transactionally, by forcing a rollback")
	@Test
	public void saveRolledBack() {
		String id = UUID.randomUUID().toString();

		assertThrowsWithCause(() -> {
			userService.run(repo -> {
				UserCol user = repo.save(new UserCol(id, "Ada", "Lovelace"));
				SimulateFailureException.throwEx("fail");
			});
		}, TransactionSystemUnambiguousException.class, SimulateFailureException.class);

		UserCol user = userRepo.getOperations().findById(UserCol.class).one(id);
		assertNull(user);
	}

	@Service // this will work in the unit tests even without @Service because of explicit loading by @SpringJUnitConfig
	static class UserService {
        @Autowired UserColRepository userRepo;

		@Transactional
		public void run(Consumer<UserColRepository> callback) {
			callback.accept(userRepo);
		}

		@Transactional
		public List<UserCol> findByFirstname(String name) {
			return userRepo.findByFirstname(name);
		}

	}

}
