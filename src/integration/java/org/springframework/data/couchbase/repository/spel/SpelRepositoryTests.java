/*
 * Copyright 2013-2019 the original author or authors.
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

package org.springframework.data.couchbase.repository.spel;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.*;

import java.util.List;

import com.couchbase.client.java.Bucket;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.repository.SimpleCouchbaseRepositoryListener;
import org.springframework.data.couchbase.repository.User;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.spi.EvaluationContextExtension;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Simon Baslé
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = SpelConfig.class)
@TestExecutionListeners(SimpleCouchbaseRepositoryListener.class)
public class SpelRepositoryTests {

  @Autowired
  private Bucket client;

  @Autowired
  private RepositoryOperationsMapping operationsMapping;

  @Autowired
  private IndexManager indexManager;

  @Autowired
  private SpelRepository repository;

  @Test
  public void testSpelExtensionResolved() {
    List<User> users = repository.findCustomUsers();
    assertEquals(1, users.size());
    assertEquals("testuser-3", users.get(0).getKey());
    assertEquals("uname-3", users.get(0).getUsername());
  }

  @Test
  public void testSpelArgumentResolution() {
    List<User> usersByName = repository.findUserWithDynamicCriteria("username", "uname-5");
    List<User> usersByAge = repository.findUserWithDynamicCriteria("age", 4);

    assertThat(usersByName, hasSize(1));
    assertThat(usersByAge, hasSize(1));
    assertThat(usersByName.get(0).getKey(), is("testuser-5"));
    assertThat(usersByAge.get(0).getKey(), is("testuser-4"));
  }

}
