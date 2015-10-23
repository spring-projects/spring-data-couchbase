/*
 * Copyright 2013 the original author or authors.
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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.CouchbaseQueryExecutionException;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Michael Nitschinger
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(SimpleCouchbaseRepositoryListener.class)
public class SimpleCouchbaseRepositoryTests {

  @Autowired
  private Bucket client;

  @Autowired
  private RepositoryOperationsMapping operationsMapping;

  @Autowired
  private IndexManager indexManager;

  private UserRepository repository;

  @Before
  public void setup() throws Exception {
    RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager);
    repository = factory.getRepository(UserRepository.class);
  }

  @Test
  public void simpleCrud() {
    String key = "my_unique_user_key";
    User instance = new User(key, "foobar", 22);
    repository.save(instance);

    User found = repository.findOne(key);
    assertEquals(instance.getKey(), found.getKey());
    assertEquals(instance.getUsername(), found.getUsername());

    assertTrue(repository.exists(key));
    repository.delete(found);

    assertNull(repository.findOne(key));
    assertFalse(repository.exists(key));
  }

  @Test
  /**
   * This test uses/assumes a default viewName called "all" that is configured on Couchbase.
   */
  public void shouldFindAll() {
    // do a non-stale query to populate data for testing.
    client.query(ViewQuery.from("user", "all").stale(Stale.FALSE));

    Iterable<User> allUsers = repository.findAll();
    int size = 0;
    for (User u : allUsers) {
      size++;
      assertNotNull(u.getKey());
      assertNotNull(u.getUsername());
    }
    assertEquals(100, size);
  }

  @Test
  public void shouldCount() {
    // do a non-stale query to populate data for testing.
    client.query(ViewQuery.from("user", "all").stale(Stale.FALSE));

    assertEquals(100, repository.count());
  }

  @Test
  @Ignore("View based query with copy of params from a ViewQuery in the method parameter not implemented")
  //TODO re-enable test once ViewQuery parameters other than designDoc/viewName can be copied
  public void shouldFindCustom() {
    Iterable<User> users = repository.customViewQuery(ViewQuery.from("", "").limit(2).stale(Stale.FALSE));
    int size = 0;
    for (User u : users) {
      size++;
      assertNotNull(u.getKey());
      assertNotNull(u.getUsername());
    }
    assertEquals(2, size);
  }

  @Test
  public void shouldFindByUsernameUsingN1ql() {
    User user = repository.findByUsername("uname-1");
    assertNotNull(user);
    assertEquals("testuser-1", user.getKey());
    assertEquals("uname-1", user.getUsername());
  }

  @Test
  public void shouldFailFindByUsernameWithNoIdOrCas() {
    try {
      User user = repository.findByUsernameBadSelect("uname-1");
      fail("shouldFailFindByUsernameWithNoIdOrCas");
    } catch (CouchbaseQueryExecutionException e) {
      assertTrue(e.getMessage().contains("_ID"));
      assertTrue(e.getMessage().contains("_CAS"));
    } catch (Exception e) {
      fail("CouchbaseQueryExecutionException expected");
    }
  }

  @Test
  public void shouldFindFromUsernameInlineWithSpelParsing() {
    User user = repository.findByUsernameWithSpelAndPlaceholder();
    assertNotNull(user);
    assertEquals("testuser-4", user.getKey());
    assertEquals("uname-4", user.getUsername());
  }

  @Test
  public void shouldFindFromDeriveQueryWithRegexpAndIn() {
    User user = repository.findByUsernameRegexAndUsernameIn("uname-[123]", Arrays.asList("uname-2", "uname-4"));
    assertNotNull(user);
    assertEquals("testuser-2", user.getKey());
    assertEquals("uname-2", user.getUsername());
  }

  @Test
  public void shouldFindContainsWithoutAnnotation() {
    List<User> users = repository.findByUsernameContains("-9");
    assertNotNull(users);
    assertFalse(users.isEmpty());
    for (User user : users) {
      assertTrue(user.getUsername().startsWith("uname-9"));
    }
  }

  @Test
  public void shouldDefaultToN1qlQueryDerivation() {
    try {
      User u = repository.findByUsernameNear("london");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      if (!e.getMessage().contains("N1QL")) {
        fail(e.getMessage());
      }
    }
  }
}
