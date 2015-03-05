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

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.ViewDesign;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

/**
 * @author Michael Nitschinger
 */
public class CouchbaseRepositoryViewListener extends DependencyInjectionTestExecutionListener {

  @Override
  public void beforeTestClass(final TestContext testContext) throws Exception {
    CouchbaseClient client = (CouchbaseClient) testContext.getApplicationContext().getBean("couchbaseClient");
    populateTestData(client);
    createAndWaitForDesignDocs(client);
  }

  private void populateTestData(final CouchbaseClient client) {
    CouchbaseTemplate template = new CouchbaseTemplate(client);
    for (int i = 0; i < 100; i++) {
      template.save(new User("testuser-" + i, "uname-" + i), PersistTo.MASTER, ReplicateTo.ZERO);
    }
  }

  private void createAndWaitForDesignDocs(final CouchbaseClient client) {
    DesignDocument designDoc = new DesignDocument("user");
    String mapFunction = "function (doc, meta) { if(doc._class == \"org.springframework.data.couchbase.repository.User\") { emit(null, null); } }";
    designDoc.setView(new ViewDesign("customFindAllView", mapFunction, "_count"));

    client.createDesignDoc(designDoc);

    designDoc = new DesignDocument("userCustom");
    designDoc.setView(new ViewDesign("customCountView", mapFunction, "_count"));

    client.createDesignDoc(designDoc);
  }

}
