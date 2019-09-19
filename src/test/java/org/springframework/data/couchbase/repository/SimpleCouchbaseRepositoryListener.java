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

package org.springframework.data.couchbase.repository;

import java.util.Collections;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

/**
 * @author Michael Nitschinger
 */
public class SimpleCouchbaseRepositoryListener extends DependencyInjectionTestExecutionListener {

  @Override
  public void beforeTestClass(final TestContext testContext) throws Exception {
    Cluster cluster = (Cluster) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_CLUSTER);
    Bucket bucket = (Bucket) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_BUCKET);
    Collection collection = bucket.defaultCollection(); // TODO: add better collection support when 6.5 is out?
    populateTestData(cluster, collection);
    cluster.queryIndexes().createPrimaryIndex(bucket.name());
  }

  private void populateTestData(Cluster cluster, Collection collection) {
    CouchbaseTemplate template = new CouchbaseTemplate(cluster, collection);

    for (int i = 0; i < 100; i++) {
      User u = new User("testuser-" + i, "uname-" + i, i);
      template.save(u, PersistTo.ACTIVE, ReplicateTo.NONE);
    }

  }
}
