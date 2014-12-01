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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;

/**
 * @author David Harrigan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
@TestExecutionListeners(CouchbaseRepositoryViewListener.class)
public class CouchbaseRepositoryViewTests {

  @Autowired
  private Bucket client;

  @Autowired
  private CouchbaseTemplate template;

  private CustomUserRepository repository;

  @Before
  public void setup() throws Exception {
    repository = new CouchbaseRepositoryFactory(template).getRepository(CustomUserRepository.class);
  }

  @Test
  public void shouldFindAllWithCustomView() {
    ViewQuery query = ViewQuery.from("user", "customFindAllView").stale(Stale.FALSE);
    client.query(query);
    Iterable<User> allUsers = repository.findAll();
    int i = 0;
    for (final User allUser : allUsers) {
      i++;
    }
    assertThat(i, is(100));
  }

  @Test
  public void shouldCountWithCustomView() {
    ViewQuery query = ViewQuery.from("userCustom", "customCountView").stale(Stale.FALSE);
    client.query(query);
    final long value = repository.count();
    assertThat(value, is(100L));
  }

  @Test(expected = InvalidDataAccessResourceUsageException.class)
  public void shouldTrimOffFindOnCustomFinder() {
    repository.findAllSomething();
  }

}
