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

package org.springframework.data.couchbase.repository.spel;

import java.util.List;

import com.couchbase.client.java.Bucket;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.repository.SimpleCouchbaseRepositoryListener;
import org.springframework.data.couchbase.repository.User;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = SpelConfig.class)
@TestExecutionListeners(SimpleCouchbaseRepositoryListener.class)
public class SpelRepositoryIntegrationTests {

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
    assertThat(users.size()).isEqualTo(1);
    assertThat(users.get(0).getKey()).isEqualTo("testuser-3");
    assertThat(users.get(0).getUsername()).isEqualTo("uname-3");
  }

  @Test
  public void testSpelArgumentResolution() {
    List<User> usersByName = repository.findUserWithDynamicCriteria("username", "uname-5");
    List<User> usersByAge = repository.findUserWithDynamicCriteria("age", 4);

    assertThat(usersByName).hasSize(1);
    assertThat(usersByAge).hasSize(1);
    assertThat(usersByName.get(0).getKey()).isEqualTo("testuser-5");
    assertThat(usersByAge.get(0).getKey()).isEqualTo("testuser-4");
  }

}
