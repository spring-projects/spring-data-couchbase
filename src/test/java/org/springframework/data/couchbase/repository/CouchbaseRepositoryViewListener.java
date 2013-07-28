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
import org.springframework.context.ApplicationContext;
import org.springframework.data.couchbase.core.Beer;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.AbstractTestExecutionListener;

import java.net.URI;
import java.util.Arrays;

/**
 * @author Michael Nitschinger
 */
public class CouchbaseRepositoryViewListener extends AbstractTestExecutionListener {

  @Override
  public void beforeTestClass(final TestContext testContext) throws Exception {
    CouchbaseClient client = bootstrapClient(testContext.getApplicationContext());
    populateTestData(client);
    createAndWaitForDesignDocs(client);
  }

  private CouchbaseClient bootstrapClient(ApplicationContext context) throws Exception {
    String host = (String) context.getBean("couchbaseHost");
    String bucket = (String) context.getBean("couchbaseBucket");
    String password = (String) context.getBean("couchbasePassword");

    return new CouchbaseClient(Arrays.asList(new URI(host)), bucket, password);
  }

  private void populateTestData(CouchbaseClient client) {
    CouchbaseTemplate template = new CouchbaseTemplate(client);

    for(int i=0;i < 100; i++) {
      User u = new User("testuser-" + i, "uname" + i);
      template.save(u);
    }
  }

  private void createAndWaitForDesignDocs(CouchbaseClient client) {
    DesignDocument designDoc = new DesignDocument("user");
    String mapFunction = "function (doc, meta) { if(doc._class == "
      + "\"org.springframework.data.couchbase.repository.User\") { emit(null, null); } }";
    designDoc.setView(new ViewDesign("all", mapFunction, "_count"));
    client.createDesignDoc(designDoc);
  }

}
