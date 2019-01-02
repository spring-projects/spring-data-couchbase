/*
 * Copyright 2013-2019 the original author or authors.
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

import java.util.Collections;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.View;

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
    Bucket client = (Bucket) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_BUCKET);
    ClusterInfo clusterInfo = (ClusterInfo) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_CLUSTER_INFO);
    populateTestData(client, clusterInfo);
    createAndWaitForDesignDocs(client);
  }

  private void populateTestData(Bucket client, ClusterInfo clusterInfo) {
    CouchbaseTemplate template = new CouchbaseTemplate(clusterInfo, client);

    for (int i = 0; i < 100; i++) {
      User u = new User("testuser-" + i, "uname-" + i, i);
      template.save(u, PersistTo.MASTER, ReplicateTo.NONE);
    }

  }

  private void createAndWaitForDesignDocs(Bucket client) {
    String mapFunction = "function (doc, meta) { if(doc._class == \"org.springframework.data.couchbase.repository." +
      "User\") { emit(null, null); } }";
    View view = DefaultView.create("all", mapFunction, "_count");
    List<View> views = Collections.singletonList(view);
    DesignDocument designDoc = DesignDocument.create("user", views);
    client.bucketManager().upsertDesignDocument(designDoc);
  }

}
