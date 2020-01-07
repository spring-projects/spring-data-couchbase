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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.springframework.data.couchbase.CouchbaseTestHelper.getRepositoryWithRetry;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.AsyncUtils;
import org.springframework.data.couchbase.core.CouchbaseQueryExecutionException;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;

/**
 * @author Michael Nitschinger
 * @author Mark Paluch
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(SimpleCouchbaseRepositoryListener.class)
public class SimpleCouchbaseRepositoryIntegrationTests {

  @Rule
  public TestName testName = new TestName();

  @Autowired
  private Bucket client;

  @Autowired
  private RepositoryOperationsMapping operationsMapping;

  @Autowired
  private IndexManager indexManager;

  private UserRepository repository;
  private VersionedDataRepository versionedDataRepository;

  @Before
  public void setup() throws Exception {
    RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager);
    repository = getRepositoryWithRetry(factory, UserRepository.class);
    versionedDataRepository = getRepositoryWithRetry(factory, VersionedDataRepository.class);
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
    User instance = new User(key, "foobar", 22);
    repository.save(instance);

    Optional<User> found = repository.findById(key);
    assertThat(found.isPresent()).isTrue();

    found.ifPresent(actual -> {
      assertThat(actual.getKey()).isEqualTo(instance.getKey());
      assertThat(actual.getUsername()).isEqualTo(instance.getUsername());

      assertThat(repository.existsById(key)).isTrue();
      repository.delete(actual);
    });

    assertThat(repository.findById(key).isPresent()).isFalse();
    assertThat(repository.existsById(key)).isFalse();
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
      assertThat(u.getKey()).isNotNull();
      assertThat(u.getUsername()).isNotNull();
    }
    assertThat(size).isEqualTo(100);
  }

  @Test
  public void shouldCount() {
    // do a non-stale query to populate data for testing.
    client.query(ViewQuery.from("user", "all").stale(Stale.FALSE));

    assertThat(repository.count()).isEqualTo(100);
  }

  @Test
  @Ignore("View based query with copy of params from a ViewQuery in the method parameter not implemented")
  //TODO re-enable test once ViewQuery parameters other than designDoc/viewName can be copied
  public void shouldFindCustom() {
    Iterable<User> users = repository.customViewQuery(ViewQuery.from("", "").limit(2).stale(Stale.FALSE));
    int size = 0;
    for (User u : users) {
      size++;
      assertThat(u.getKey()).isNotNull();
      assertThat(u.getUsername()).isNotNull();
    }
    assertThat(size).isEqualTo(2);
  }

  @Test
  public void shouldFindByUsernameUsingN1ql() {
    User user = repository.findByUsername("uname-1");
    assertThat(user).isNotNull();
    assertThat(user.getKey()).isEqualTo("testuser-1");
    assertThat(user.getUsername()).isEqualTo("uname-1");
  }

  @Test
  public void shouldFailFindByUsernameWithNoIdOrCas() {
    try {
      User user = repository.findByUsernameBadSelect("uname-1");
      fail("shouldFailFindByUsernameWithNoIdOrCas");
    } catch (CouchbaseQueryExecutionException e) {
      assertThat(e.getMessage().contains("_ID")).as("_ID expected in exception " + e)
			  .isTrue();
      assertThat(e.getMessage().contains("_CAS")).as("_CAS expected in exception " + e)
			  .isTrue();
    } catch (Exception e) {
      fail("CouchbaseQueryExecutionException expected");
    }
  }

  @Test
  public void shouldFindFromUsernameInlineWithSpelParsing() {
    User user = repository.findByUsernameWithSpelAndPlaceholder();
    assertThat(user).isNotNull();
    assertThat(user.getKey()).isEqualTo("testuser-4");
    assertThat(user.getUsername()).isEqualTo("uname-4");
  }

  @Test
  public void shouldFindFromDeriveQueryWithRegexpAndIn() {
    User user = repository.findByUsernameRegexAndUsernameIn("uname-[123]", Arrays.asList("uname-2", "uname-4"));
    assertThat(user).isNotNull();
    assertThat(user.getKey()).isEqualTo("testuser-2");
    assertThat(user.getUsername()).isEqualTo("uname-2");
  }

  @Test
  public void shouldFindContainsWithoutAnnotation() {
    List<User> users = repository.findByUsernameContains("-9");
    assertThat(users).isNotNull();
    assertThat(users.isEmpty()).isFalse();
    for (User user : users) {
      assertThat(user.getUsername().startsWith("uname-9")).isTrue();
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

  @Test
  public void shouldTakeVersionIntoAccountWhenDoingMultipleUpdates() {
    final String key = "versionedUserTest";
    VersionedData initial = new VersionedData(key, "ABCD");
    versionedDataRepository.save(initial);
    assertThat(initial.version).isNotEqualTo(0L);

    Optional<VersionedData> fetch1 = versionedDataRepository.findById(key);

    assertThat(fetch1.isPresent()).isTrue();
    fetch1.ifPresent(actual -> {
      assertThat(actual).isNotSameAs(initial);
      assertThat(initial.version).isEqualTo(actual.version);
    });

    VersionedData versionedData = fetch1.get();

    JsonDocument bypass = client.get(key);
    bypass.content().put("data", "BBBB");
    JsonDocument bypassed = client.upsert(bypass);

    assertThat(versionedData.version).isNotEqualTo(bypassed.cas());
    System.out.println(bypassed.cas());

    try {
      versionedData.setData("ZZZZ");
      versionedDataRepository.save(versionedData);
      fail("Expected CAS failure");
    }  catch (OptimisticLockingFailureException e) {
      //success
      assertThat(e.getCause() instanceof CASMismatchException)
			  .as("optimistic locking should have CASMismatchException as cause, got " + e
					  .getCause()).isTrue();
    } finally {
      client.remove(key);
    }
  }

  // todo: investigate cause of intermittent failure
  @Ignore("Fails intermittently (see DATACOUCH-452)")
  @Test
  public void shouldUpdateDocumentConcurrently() throws Exception {
    final String key = testName.getMethodName();
    remove(key);

    final AtomicLong counter = new AtomicLong();
    final AtomicLong updatedCounter = new AtomicLong();
    VersionedData initial = new VersionedData(key, "value-initial");
    versionedDataRepository.save(initial);
    assertThat(initial.version).isNotEqualTo(0L);

    Callable<Void> task = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        boolean updated = false;
        while(!updated) {
          long counterValue = counter.incrementAndGet();
          VersionedData messageData = versionedDataRepository.findById(key).get();
          messageData.data = "value-" + counterValue;
          try {
            versionedDataRepository.save(messageData);
            updated = true;
            updatedCounter.incrementAndGet();
          } catch (OptimisticLockingFailureException e) {
          }
        }
        return null;
      }
    };
    AsyncUtils.executeConcurrently(5, task);

    assertThat(versionedDataRepository.findById(key).get().data)
			.isNotEqualTo(initial.data);
    assertThat(updatedCounter.intValue()).isEqualTo(5);
  }

  @Test
  public void shouldFailOnMultipleConcurrentSaves() throws Exception {
    final String key = testName.getMethodName();
    remove(key);

    final AtomicLong counter = new AtomicLong();
    final AtomicLong optimisticLockCounter = new AtomicLong();

    Callable<Void> task = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        long counterValue = counter.incrementAndGet();
        VersionedData messageData = new VersionedData(key, "value-" + counterValue);
        try {
          versionedDataRepository.save(messageData);
        } catch (OptimisticLockingFailureException e) {
          optimisticLockCounter.incrementAndGet();
        }
        return null;
      }
    };

    AsyncUtils.executeConcurrently(5, task);

    assertThat(optimisticLockCounter.intValue()).isEqualTo(4);
  }


  public interface VersionedDataRepository extends CouchbaseRepository<VersionedData, String> { }

  @Document
  public static class VersionedData {

    @Id
    private final String key;

    @Version
    public long version = 0L;

    private String data;

    public VersionedData(String key, String data) {
      this.key = key;
      this.data = data;
    }

    public String getKey() {
      return key;
    }

    public String getData() {
      return data;
    }

    public void setData(String data) {
      this.data = data;
    }

    @Override
    public String toString() {
      return this.key + " " + this.data;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      VersionedData vd = (VersionedData) o;
      return key.equals(vd.key);
    }

    @Override
    public int hashCode() {
      return key.hashCode();
    }
  }

}
