/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.ReactiveIntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.CouchbaseQueryExecutionException;
import org.springframework.data.couchbase.repository.config.ReactiveRepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.ReactiveCouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.repository.core.support.ReactiveRepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Subhashni Balakrishnan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ReactiveIntegrationTestApplicationConfig.class)
@TestExecutionListeners(SimpleReactiveCouchbaseRepositoryListener.class)
public class SimpleReactiveCouchbaseRepositoryTests {

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
		repository = factory.getRepository(ReactiveUserRepository.class);
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

		ReactiveUser found = repository.findOne(key).block();
		assertEquals(instance.getKey(), found.getKey());
		assertEquals(instance.getUsername(), found.getUsername());

		assertTrue(repository.exists(key).block());
		repository.delete(found).block();

		assertNull(repository.findOne(key).block());
		assertFalse(repository.exists(key).block());
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
			assertNotNull(u.getKey());
			assertNotNull(u.getUsername());
		}
		assertEquals(100, size);
	}

	@Test
	public void shouldCount() {
		// do a non-stale query to populate data for testing.
		client.query(ViewQuery.from("reactiveUser", "all").stale(Stale.FALSE));

		assertEquals("100", repository.count().block().toString());
	}

	@Test
	public void shouldFindByUsernameUsingN1ql() {
		ReactiveUser user = repository.findByUsername("reactiveuname-1").single().block();
		assertNotNull(user);
		assertEquals("reactivetestuser-1", user.getKey());
		assertEquals("reactiveuname-1", user.getUsername());
	}

	@Test
	public void shouldFailFindByUsernameWithNoIdOrCas() {
		try {
			ReactiveUser user = repository.findByUsernameBadSelect("reactiveuname-1").single().block();
			fail("shouldFailFindByUsernameWithNoIdOrCas");
		} catch (CouchbaseQueryExecutionException e) {
			assertTrue("_ID expected in exception " + e, e.getMessage().contains("_ID"));
			assertTrue("_CAS expected in exception " + e, e.getMessage().contains("_CAS"));
		} catch (Exception e) {
			fail("CouchbaseQueryExecutionException expected");
		}
	}

	@Test
	public void shouldFindFromUsernameInlineWithSpelParsing() {
		ReactiveUser user = repository.findByUsernameWithSpelAndPlaceholder().take(1).blockLast();
		assertNotNull(user);
		assert(user.getUsername().startsWith("reactive"));
		assert(user.getUsername().startsWith("reactive"));
	}

	@Test
	public void shouldFindFromDeriveQueryWithRegexpAndIn() {
		ReactiveUser user = repository.findByUsernameRegexAndUsernameIn("reactiveuname-[123]", Arrays.asList("reactiveuname-2", "reactiveuname-4")).take(1).blockLast();
		assertNotNull(user);
		assertEquals("reactivetestuser-2", user.getKey());
		assertEquals("reactiveuname-2", user.getUsername());
	}

	@Test
	public void shouldFindContainsWithoutAnnotation() {
		List<ReactiveUser> users = repository.findByUsernameContains("reactive").collectList().block();
		assertNotNull(users);
		assertFalse(users.isEmpty());
		for (ReactiveUser user : users) {
			assertTrue(user.getUsername().startsWith("reactive"));
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
