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

package org.springframework.data.couchbase.repository.extending.method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * This tests custom repository methods.
 *
 * @author Simon Baslé
 */
@SuppressWarnings("SpringJavaAutowiringInspection")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RepositoryCustomMethodTest {

  @Autowired
  MyRepository repository;

  @Configuration
  @EnableCouchbaseRepositories
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

    //this is for dev so it is ok to auto-create indexes
    @Override
    public IndexManager indexManager() {
      return new IndexManager();
    }

    @Override
    protected Consistency getDefaultConsistency() {
      return Consistency.STRONGLY_CONSISTENT;
    }
  }

  private static final String KEY = "customMethodTestItem";

  @Before
  public void initData() {
    try { repository.delete(KEY); } catch (Exception e) { }
    repository.save(new MyItem(KEY, "new item for custom count"));
  }

  @After
  public void clearData() {
    repository.delete(KEY);
  }

  @Test
  public void testRepositoryCustomMethodIsWeavedIn() {
    long customCount = repository.customCountItems();
    assertEquals(-1L, customCount);
  }

  @Test
  public void testRepositoryCrudMethodIsReplaced() {
    long count = repository.count();
    assertEquals(100L, count);
  }

}
