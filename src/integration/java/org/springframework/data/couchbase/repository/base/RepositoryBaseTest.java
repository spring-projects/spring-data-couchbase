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

package org.springframework.data.couchbase.repository.base;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.util.features.CouchbaseFeature;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.base.impl.MyRepository;
import org.springframework.data.couchbase.repository.base.impl.MyRepositoryImpl;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * This tests custom implementation of base repository.
 *
 * @author Simon Baslé
 */
@SuppressWarnings("SpringJavaAutowiringInspection")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RepositoryBaseTest {

  private static CouchbaseOperations mockOpsA;

  @BeforeClass
  public static void initMocks() {
    ClusterInfo info = mock(ClusterInfo.class);
    when(info.checkAvailable(any(CouchbaseFeature.class))).thenReturn(true);

    mockOpsA = mock(CouchbaseOperations.class);
    when(mockOpsA.getCouchbaseClusterInfo()).thenReturn(info);
    when(mockOpsA.exists(any(String.class))).thenReturn(true);
  }

  @Autowired
  ItemRepository repositoryA;

  public interface ItemRepository extends MyRepository<Item, String> {
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
      return "default";
    }

    @Override
    protected String getBucketPassword() {
      return "";
    }

    @Bean
    public CouchbaseOperations couchbaseOperations() {
      return mockOpsA;
    }
  }

  @Test
  public void testRepositoryBaseIsChanged() {
    assertNotNull(repositoryA);

    assertEquals(4, repositoryA.sharedCustomMethod("toto"));
    assertEquals(4000, repositoryA.sharedCustomMethod("anna"));
  }

  private static class Item {
    @Id
    public String id;

    public String value;
  }

}
