/*
 * Copyright 2013-2017 the original author or authors.
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
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.error.CASMismatchException;
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
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.AsyncUtils;
import org.springframework.data.couchbase.core.CouchbaseQueryExecutionException;
import org.springframework.data.couchbase.core.CouchbaseTemplateTests;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * @author Michael Nitschinger
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(SimpleCouchbaseRepositoryListener.class)
public class SimpleCouchbaseRepositoryTests {

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
    repository = factory.getRepository(UserRepository.class);
    versionedDataRepository = factory.getRepository(VersionedDataRepository.class);
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

    Optional<User> found = repository.findOne(key);
    assertTrue(found.isPresent());

    found.ifPresent(actual -> {
      assertEquals(instance.getKey(), actual.getKey());
      assertEquals(instance.getUsername(), actual.getUsername());

      assertTrue(repository.exists(key));
      repository.delete(actual);
    });

    assertFalse(repository.findOne(key).isPresent());
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
      assertTrue("_ID expected in exception " + e, e.getMessage().contains("_ID"));
      assertTrue("_CAS expected in exception " + e, e.getMessage().contains("_CAS"));
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

  @Test
  public void shouldTakeVersionIntoAccountWhenDoingMultipleUpdates() {
    final String key = "versionedUserTest";
    VersionedData initial = new VersionedData(key, "ABCD");
    versionedDataRepository.save(initial);
    assertNotEquals(0L, initial.version);

    Optional<VersionedData> fetch1 = versionedDataRepository.findOne(key);

    assertTrue(fetch1.isPresent());
    fetch1.ifPresent(actual -> {
      assertNotSame(initial, actual);
      assertEquals(actual.version, initial.version);
    });

    VersionedData versionedData = fetch1.get();

    JsonDocument bypass = client.get(key);
    bypass.content().put("data", "BBBB");
    JsonDocument bypassed = client.upsert(bypass);

    assertNotEquals(bypassed.cas(), versionedData.version);
    System.out.println(bypassed.cas());

    try {
      versionedData.setData("ZZZZ");
      versionedDataRepository.save(versionedData);
      fail("Expected CAS failure");
    }  catch (OptimisticLockingFailureException e) {
      //success
      assertTrue("optimistic locking should have CASMismatchException as cause, got " + e.getCause(),
          e.getCause() instanceof CASMismatchException);
    } finally {
      client.remove(key);
    }
  }

  @Test
  public void shouldUpdateDocumentConcurrently() throws Exception {
    final String key = testName.getMethodName();
    remove(key);

    final AtomicLong counter = new AtomicLong();
    final AtomicLong updatedCounter = new AtomicLong();
    VersionedData initial = new VersionedData(key, "value-initial");
    versionedDataRepository.save(initial);
    assertNotEquals(0L, initial.version);

    Callable<Void> task = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        boolean updated = false;
        while(!updated) {
          long counterValue = counter.incrementAndGet();
          VersionedData messageData = versionedDataRepository.findOne(key).get();
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

    assertNotEquals(initial.data, versionedDataRepository.findOne(key).get().data);
    assertEquals(5, updatedCounter.intValue());
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

    assertEquals(4, optimisticLockCounter.intValue());
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
