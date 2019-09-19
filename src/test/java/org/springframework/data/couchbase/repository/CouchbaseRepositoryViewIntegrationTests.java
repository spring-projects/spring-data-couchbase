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

package org.springframework.data.couchbase.repository;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.json.JsonObject;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;

/**
 * @author David Harrigan
 * @author Simon Baslé
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(CouchbaseRepositoryViewListener.class)
public class CouchbaseRepositoryViewIntegrationTests {

  @Autowired
  private Bucket client;

  @Autowired
  private RepositoryOperationsMapping operationsMapping;

  @Autowired
  private IndexManager indexManager;

  private CustomUserRepository repository;

  @Before
  public void setup() throws Exception {
    repository = new CouchbaseRepositoryFactory(operationsMapping, indexManager).getRepository(CustomUserRepository.class);
  }

  @Test
  public void shouldDetectMethodNameWithoutPropertyAndIssueGenericQueryOnView() {
    Iterable<User> users = repository.findRandomMethodName();
    assertNotNull(users);
    assertTrue(users.iterator().hasNext());

    try {
      repository.findIncorrectExplicitView();
      fail("Expected InvalidDataAccessResourceException");
    } catch (InvalidDataAccessResourceUsageException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("View user/allSomething does not exist"));
    }
  }

  @Test(expected = PropertyReferenceException.class)
  public void shouldFailDeriveOnBadProperty() {
    repository.findAllByUsernameEqualAndUserblablaIs("uname-1", "blabla");
  }

  @Test
  public void shouldDeriveViewParametersAndReduce() {
    long count = repository.countByUsernameGreaterThanEqualAndUsernameLessThan("uname-8", "uname-9");
    assertEquals(12, count);
  }

  @Test
  public void shouldDeriveViewParametersAndReduceNonNumerical() {
    JsonObject reduceResult = repository.findByAgeLessThan(50);

    assertNotNull(reduceResult);
    assertEquals(51, (long) reduceResult.getLong("count"));
    assertEquals(50, (long) reduceResult.getLong("max"));
    assertEquals(0, (long) reduceResult.getLong("min"));
    assertEquals(1275, (long) reduceResult.getLong("sum"));
  }

  @Test
  public void shouldDeriveViewParameters() {
    String lowKey = "uname-1";
    String middleKey = "uname-10";
    String highKey = "uname-11";
    List<String> keys = Arrays.asList(lowKey, middleKey, highKey);

    User u1 = repository.findByUsernameIs(lowKey).get(0);
    User u2 = repository.findByUsernameIs(middleKey).get(0);
    User u3 = repository.findByUsernameIs(highKey).get(0);

    assertEquals(lowKey, u1.getUsername());
    assertEquals(middleKey, u2.getUsername());
    assertEquals(highKey, u3.getUsername());

    List<User> in = repository.findAllByUsernameIn(keys);
    List<User> gteLte = repository.findByUsernameGreaterThanEqualAndUsernameLessThanEqual(lowKey, highKey);
    List<User> between = repository.findByUsernameBetween(lowKey, highKey);
    List<User> gteLimited = repository.findTop3ByUsernameGreaterThanEqual(lowKey);

    // the results are unordered, so compare using Set
    Set<User> expected = new HashSet<>(Arrays.asList(u1, u2, u3));
    assertEquals(expected, new HashSet<>(in));
    assertEquals(expected, new HashSet<>(gteLte));
    assertEquals(expected, new HashSet<>(between));
    assertEquals(expected, new HashSet<>(gteLimited));
  }

  @Test
  public void shouldDeriveToEmptyClause() {
    List<User> users = repository.findAllByUsername();
    assertNotNull(users);
    assertEquals(100, users.size());
  }

  @Test
  public void shouldDetermineViewNameFromMethodPrefix() {
    try {
      repository.findByIncorrectView();
      fail("Expected InvalidDataAccessResourceException");
    } catch (InvalidDataAccessResourceUsageException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("View user/byIncorrectView does not exist"));
    }
  }

  @Test
  public void shouldDetermineViewNameFromCountPrefixAndReduce() {
    long count = repository.countCustomFindAllView();
    assertEquals(100, count);

    try {
      repository.countCustomFindInvalid();
      fail("Expected InvalidDataAccessResourceException");
    } catch (InvalidDataAccessResourceUsageException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("View user/customFindInvalid does not exist"));
    }
  }
}
