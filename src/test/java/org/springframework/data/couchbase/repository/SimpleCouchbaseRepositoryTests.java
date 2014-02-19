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

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * @author Michael Nitschinger
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
@TestExecutionListeners(SimpleCouchbaseRepositoryListener.class)
public class SimpleCouchbaseRepositoryTests {

  @Autowired
  private CouchbaseClient client;

  @Autowired
  private CouchbaseTemplate template;

  private UserRepository repository;

  @Before
  public void setup() throws Exception {
    RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(template);
    repository = factory.getRepository(UserRepository.class);
  }

  @Test
  public void simpleCrud() {
    String key = "my_unique_user_key";
    User instance = new User(key, "foobar");
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
    client.query(client.getView("user", "all"), new Query().setStale(Stale.FALSE));

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
    client.query(client.getView("user", "all"), new Query().setStale(Stale.FALSE));

    assertEquals(100, repository.count());
  }

  @Test
  public void shouldFindCustom() {
    Iterable<User> users = repository.customViewQuery(new Query().setLimit(2).setStale(Stale.FALSE));
    int size = 0;
    for (User u : users) {
      size++;
      assertNotNull(u.getKey());
      assertNotNull(u.getUsername());
    }
    assertEquals(2, size);
  }

}
