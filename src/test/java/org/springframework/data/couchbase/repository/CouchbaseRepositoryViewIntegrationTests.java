/*
 * Copyright 2013-2020 the original author or authors.
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
  public void shouldFindAllWithCustomView() {
    client.query(ViewQuery.from("user", "customFindAllView").stale(Stale.FALSE));
    Iterable<User> allUsers = repository.findAll();
    assertThat(allUsers).hasSize(100);
  }

  @Test
  public void shouldCountWithCustomView() {
    ViewResult clientResult = client.query(ViewQuery.from("userCustom", "customCountView")
        .reduce().stale(Stale.FALSE));
    final Object clientRowValue = clientResult.allRows().get(0).value();
    final long value = repository.count();
    assertThat(value).isEqualTo(100L);
    assertThat(clientRowValue).isInstanceOf(Number.class);
    assertThat(((Number) clientRowValue).longValue()).isEqualTo(value);
  }

  @Test
  public void shouldDetectMethodNameWithoutPropertyAndIssueGenericQueryOnView() {
    Iterable<User> users = repository.findRandomMethodName();
    assertThat(users).isNotNull();
    assertThat(users.iterator().hasNext()).isTrue();

    try {
      repository.findIncorrectExplicitView();
      fail("Expected InvalidDataAccessResourceException");
    } catch (InvalidDataAccessResourceUsageException e) {
      assertThat(e.getMessage().startsWith("View user/allSomething does not exist"))
			  .as(e.getMessage()).isTrue();
    }
  }

  @Test(expected = PropertyReferenceException.class)
  public void shouldFailDeriveOnBadProperty() {
    repository.findAllByUsernameEqualAndUserblablaIs("uname-1", "blabla");
  }

  @Test
  public void shouldDeriveViewParametersAndReduce() {
    long count = repository.countByUsernameGreaterThanEqualAndUsernameLessThan("uname-8", "uname-9");
    assertThat(count).isEqualTo(12);
  }

  @Test
  public void shouldDeriveViewParametersAndReduceNonNumerical() {
    JsonObject reduceResult = repository.findByAgeLessThan(50);

    assertThat(reduceResult).isNotNull();
    assertThat((long) reduceResult.getLong("count")).isEqualTo(51);
    assertThat((long) reduceResult.getLong("max")).isEqualTo(50);
    assertThat((long) reduceResult.getLong("min")).isEqualTo(0);
    assertThat((long) reduceResult.getLong("sum")).isEqualTo(1275);
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

    assertThat(u1.getUsername()).isEqualTo(lowKey);
    assertThat(u2.getUsername()).isEqualTo(middleKey);
    assertThat(u3.getUsername()).isEqualTo(highKey);

    List<User> in = repository.findAllByUsernameIn(keys);
    List<User> gteLte = repository.findByUsernameGreaterThanEqualAndUsernameLessThanEqual(lowKey, highKey);
    List<User> between = repository.findByUsernameBetween(lowKey, highKey);
    List<User> gteLimited = repository.findTop3ByUsernameGreaterThanEqual(lowKey);

    // the results are unordered, so compare using Set
    Set<User> expected = new HashSet<>(Arrays.asList(u1, u2, u3));
    assertThat(new HashSet<>(in)).isEqualTo(expected);
    assertThat(new HashSet<>(gteLte)).isEqualTo(expected);
    assertThat(new HashSet<>(between)).isEqualTo(expected);
    assertThat(new HashSet<>(gteLimited)).isEqualTo(expected);
  }

  @Test
  public void shouldDeriveToEmptyClause() {
    List<User> users = repository.findAllByUsername();
    assertThat(users).isNotNull();
    assertThat(users.size()).isEqualTo(100);
  }

  @Test
  public void shouldDetermineViewNameFromMethodPrefix() {
    try {
      repository.findByIncorrectView();
      fail("Expected InvalidDataAccessResourceException");
    } catch (InvalidDataAccessResourceUsageException e) {
      assertThat(e.getMessage().startsWith("View user/byIncorrectView does not exist"))
			  .as(e.getMessage()).isTrue();
    }
  }

  @Test
  public void shouldDetermineViewNameFromCountPrefixAndReduce() {
    long count = repository.countCustomFindAllView();
    assertThat(count).isEqualTo(100);

    try {
      repository.countCustomFindInvalid();
      fail("Expected InvalidDataAccessResourceException");
    } catch (InvalidDataAccessResourceUsageException e) {
      assertThat(e.getMessage().startsWith("View user/customFindInvalid does not exist"))
			  .as(e.getMessage()).isTrue();
    }
  }
}
