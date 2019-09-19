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

package org.springframework.data.couchbase.repository.extending.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.User;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.extending.base.impl.MyRepository;
import org.springframework.data.couchbase.repository.extending.base.impl.MyRepositoryImpl;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.test.context.ContextConfiguration;

/**
 * This tests custom implementation of base repository.
 *
 * @author Simon Baslé
 */
@SuppressWarnings("SpringJavaAutowiringInspection")
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration
public class RepositoryBaseIntegrationTests {

  private static CouchbaseOperations mockOpsA;

  @BeforeClass
  public static void initMocks() {
    mockOpsA = mock(CouchbaseOperations.class);
    when(mockOpsA.exists(any(String.class))).thenReturn(true);
  }

  @Autowired
  ItemRepository repositoryA;

  @Autowired
  UserRepository repositoryB;

  public interface ItemRepository extends MyRepository<Item, String> {
    //
  }

  public interface UserRepository extends MyRepository<User, String> {
    //
  }

  @Configuration
  @EnableCouchbaseRepositories(considerNestedRepositories = true, repositoryBaseClass = MyRepositoryImpl.class)
  static class Config extends AbstractCouchbaseConfiguration {

    @Override
    protected List<String> getBootstrapHosts() {
      return Arrays.asList("127.0.0.1");
    }

    @Override
    protected String getBucketName() {
      return "protected";
    }

    @Override
    protected String getBucketPassword() {
      return "password";
    }

    @Bean
    public CouchbaseOperations couchbaseOperations() {
      return mockOpsA;
    }

  }

  @Test
  public void testRepositoryBaseIsChanged() {
    assertNotNull(repositoryA);
    assertNotNull(repositoryB);

    assertEquals(4, repositoryA.sharedCustomMethod("toto"));
    assertEquals(4000, repositoryA.sharedCustomMethod("anna"));

    assertEquals(repositoryA.sharedCustomMethod("sameInput"), repositoryB.sharedCustomMethod("sameInput"));
    assertEquals(repositoryA.sharedCustomMethod("anna"), repositoryB.sharedCustomMethod("anna"));
  }

  private static class Item {
    @Id
    public String id;

    public String value;
  }

}
