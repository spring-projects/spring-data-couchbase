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

package org.springframework.data.couchbase.repository.feature;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.springframework.data.couchbase.CouchbaseTestHelper.getRepositoryWithRetry;
import static org.springframework.data.couchbase.core.query.N1QLExpression.x;

import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Cluster;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.UnsupportedCouchbaseFeatureException;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.repository.User;
import org.springframework.data.couchbase.repository.UserRepository;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;

/**
 * An integration test that validates feature checking with Java Config.
 *
 * @author Simon Baslé
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = FeatureDetectionTestApplicationConfig.class)
public class FeatureDetectionRepositoryIntegrationTests {

  @Autowired
  private RepositoryOperationsMapping operationsMapping;

  @Autowired
  private Cluster cluster;

  @Autowired
  private IndexManager indexManager;
/*
  @Before
  public void checkClusterInfo() {

    cluster.core().clusterConfig().

            Assume.assumeTrue(clusterInfo.getMinVersion() == Version.NO_VERSION);
  }

  @Test
  public void testN1qlIncompatibleClusterFailsFastForN1qlBasedRepository() throws Exception {
    RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager))
    try {
      factory.getRepository(UserRepository.class);
      fail("expected UnsupportedCouchbaseFeatureException");
    } catch (UnsupportedCouchbaseFeatureException e) {
      assertEquals(ServiceType.QUERY, e.getFeature());
    }
  }

  @Test
  public void testN1qlIncompatibleClusterDoesntFailForViewBasedRepository() throws Exception {
    RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager);
    ViewOnlyUserRepository repository = getRepositoryWithRetry(factory, ViewOnlyUserRepository.class);
    assertNotNull(repository);
  }

  @Test
  public void testN1qlIncompatibleClusterTemplateFails() {
    final CouchbaseOperations template = operationsMapping.getDefault();

    N1QLQuery query = new N1QLQuery(x("SELECT * FROM `" + template.getCouchbaseBucket().name() + "`"));
    try {
      template.findByN1QL(query, User.class);
      fail("expected findByN1QL to fail with UnsupportedCouchbaseFeatureException");
    } catch (UnsupportedCouchbaseFeatureException e) {
      assertEquals(ServiceType.QUERY, e.getFeature());
    }

    try {
      template.findByN1QLProjection(query, User.class);
      fail("expected findByN1QLProjection to fail with UnsupportedCouchbaseFeatureException");
    } catch (UnsupportedCouchbaseFeatureException e) {
      assertEquals(ServiceType.QUERY, e.getFeature());
    }

    try {
      template.queryN1QL(query);
      fail("expected queryN1QL to fail with UnsupportedCouchbaseFeatureException");
    } catch (UnsupportedCouchbaseFeatureException e) {
      assertEquals(ServiceType.QUERY, e.getFeature());
    }
  }

 */
}
