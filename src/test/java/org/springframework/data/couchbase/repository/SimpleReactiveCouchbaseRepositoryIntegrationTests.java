/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.springframework.data.couchbase.CouchbaseTestHelper.getRepositoryWithRetry;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.ReactiveIntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.CouchbaseQueryExecutionException;
import org.springframework.data.couchbase.repository.config.ReactiveRepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.couchbase.repository.support.ReactiveCouchbaseRepositoryFactory;
import org.springframework.data.repository.core.support.ReactiveRepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;

/**
 * @author Subhashni Balakrishnan
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = ReactiveIntegrationTestApplicationConfig.class)
@TestExecutionListeners(SimpleReactiveCouchbaseRepositoryListener.class)
public class SimpleReactiveCouchbaseRepositoryIntegrationTests {

	@Rule
	public TestName testName = new TestName();

	@Autowired
	private Bucket client;

	@Autowired
	private ReactiveRepositoryOperationsMapping operationsMapping;

	@Autowired
	private IndexManager indexManager;

	private ReactiveUserRepository repository;

	@Before
	public void setup() throws Exception {
		ReactiveRepositoryFactorySupport factory = new ReactiveCouchbaseRepositoryFactory(operationsMapping, indexManager);
		repository = getRepositoryWithRetry(factory, ReactiveUserRepository.class);
	}

	private void remove(String key) {
		try {
			client.remove(key);
		} catch (DocumentDoesNotExistException e) {
		}
	}

	@Test
	public void simpleCrud() {
		String key = "my_unique_user_key";
		ReactiveUser instance = new ReactiveUser(key, "foobar", 22);
		repository.save(instance).block();

		ReactiveUser found = repository.findById(key).block();
		assertThat(found.getKey()).isEqualTo(instance.getKey());
		assertThat(found.getUsername()).isEqualTo(instance.getUsername());

		assertThat(repository.existsById(key).block()).isTrue();
		repository.delete(found).block();

		assertThat(repository.findById(key).block()).isNull();
		assertThat(repository.existsById(key).block()).isFalse();
	}

	@Test
	/**
	 * This test uses/assumes a default viewName called "all" that is configured on Couchbase.
	 */
	public void shouldFindAll() {
		// do a non-stale query to populate data for testing.
		client.query(ViewQuery.from("reactiveUser", "all").stale(Stale.FALSE));

		List<ReactiveUser> allUsers = repository.findAll().collectList().block();
		int size = 0;
		for (ReactiveUser u : allUsers) {
			size++;
			assertThat(u.getKey()).isNotNull();
			assertThat(u.getUsername()).isNotNull();
		}
		assertThat(size).isEqualTo(100);
	}

	@Test
	public void shouldCount() {
		// do a non-stale query to populate data for testing.
		client.query(ViewQuery.from("reactiveUser", "all").stale(Stale.FALSE));

		assertThat(repository.count().block().toString()).isEqualTo("100");
	}

	@Test
	public void shouldFindByUsernameUsingN1ql() {
		ReactiveUser user = repository.findByUsername("reactiveuname-1").single().block();
		assertThat(user).isNotNull();
		assertThat(user.getKey()).isEqualTo("reactivetestuser-1");
		assertThat(user.getUsername()).isEqualTo("reactiveuname-1");
	}

	@Test
	public void shouldFailFindByUsernameWithNoIdOrCas() {
		try {
			ReactiveUser user = repository.findByUsernameBadSelect("reactiveuname-1").single().block();
			fail("shouldFailFindByUsernameWithNoIdOrCas");
		} catch (CouchbaseQueryExecutionException e) {
			assertThat(e.getMessage().contains("_ID"))
					.as("_ID expected in exception " + e).isTrue();
			assertThat(e.getMessage().contains("_CAS"))
					.as("_CAS expected in exception " + e).isTrue();
		} catch (Exception e) {
			fail("CouchbaseQueryExecutionException expected");
		}
	}

	@Test
	public void shouldFindFromUsernameInlineWithSpelParsing() {
		ReactiveUser user = repository.findByUsernameWithSpelAndPlaceholder().take(1).blockLast();
		assertThat(user).isNotNull();
		assert(user.getUsername().startsWith("reactive"));
		assert(user.getUsername().startsWith("reactive"));
	}

	@Test
	public void shouldFindFromDeriveQueryWithRegexpAndIn() {
		ReactiveUser user = repository.findByUsernameRegexAndUsernameIn("reactiveuname-[123]", Arrays.asList("reactiveuname-2", "reactiveuname-4")).take(1).blockLast();
		assertThat(user).isNotNull();
		assertThat(user.getKey()).isEqualTo("reactivetestuser-2");
		assertThat(user.getUsername()).isEqualTo("reactiveuname-2");
	}

	@Test
	public void shouldFindContainsWithoutAnnotation() {
		List<ReactiveUser> users = repository.findByUsernameContains("reactive").collectList().block();
		assertThat(users).isNotNull();
		assertThat(users.isEmpty()).isFalse();
		for (ReactiveUser user : users) {
			assertThat(user.getUsername().startsWith("reactive")).isTrue();
		}
	}

	@Test
	public void shouldDefaultToN1qlQueryDerivation() {
		try {
			ReactiveUser u = repository.findByUsernameNear("london").single().block();
			fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			if (!e.getMessage().contains("N1QL")) {
				fail(e.getMessage());
			}
		}
	}




}
