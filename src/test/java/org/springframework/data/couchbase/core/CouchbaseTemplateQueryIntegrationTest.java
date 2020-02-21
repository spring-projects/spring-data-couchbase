/*
 * Copyright 2012-2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core;

import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTest;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
class CouchbaseTemplateQueryIntegrationTest extends ClusterAwareIntegrationTest {

  private CouchbaseTemplate couchbaseTemplate;

  private static CouchbaseClientFactory couchbaseClientFactory;

  @BeforeAll
  static void beforeAll() {
    couchbaseClientFactory = new SimpleCouchbaseClientFactory(connectionString(), authenticator(), bucketName());
  }

  @AfterAll
  static void afterAll() throws IOException {
    couchbaseClientFactory.close();
  }

  @BeforeEach
  void beforeEach() {
    CouchbaseConverter couchbaseConverter = new MappingCouchbaseConverter();
    couchbaseTemplate = new CouchbaseTemplate(couchbaseClientFactory, couchbaseConverter);

    try {
      couchbaseClientFactory.getCluster().queryIndexes().createPrimaryIndex(bucketName());
    } catch (IndexExistsException ex) {
      // ignore, all good.
    }
  }

  @Test
  void findByQuery() {
    User user1 = new User(UUID.randomUUID().toString(), "user1", "user1");
    User user2 = new User(UUID.randomUUID().toString(), "user2", "user2");

    couchbaseTemplate.upsertById(User.class).all(Arrays.asList(user1, user2));

    final List<User> foundUsers = couchbaseTemplate
      .findByQuery(User.class)
      .consistentWith(QueryScanConsistency.REQUEST_PLUS)
      .all();

    assertEquals(2, foundUsers.size());
    for (User u : foundUsers) {
      assertTrue(u.equals(user1) || u.equals(user2));
    }
  }

  @Test
  void removeByQuery() {
    User user1 = new User(UUID.randomUUID().toString(), "user1", "user1");
    User user2 = new User(UUID.randomUUID().toString(), "user2", "user2");

    couchbaseTemplate.upsertById(User.class).all(Arrays.asList(user1, user2));

    assertTrue(couchbaseTemplate.existsById().one(user1.getId()));
    assertTrue(couchbaseTemplate.existsById().one(user2.getId()));

    couchbaseTemplate.removeByQuery(User.class).consistentWith(QueryScanConsistency.REQUEST_PLUS).all();

    assertThrows(DataRetrievalFailureException.class, () -> couchbaseTemplate.findById(User.class).one(user1.getId()));
    assertThrows(DataRetrievalFailureException.class, () -> couchbaseTemplate.findById(User.class).one(user2.getId()));
  }

}
