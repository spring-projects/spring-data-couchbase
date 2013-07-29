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

package org.springframework.data.couchbase.core;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.ViewDesign;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

/**
 * @author Michael Nitschinger
 */
public class CouchbaseTemplateViewListener extends DependencyInjectionTestExecutionListener {

  @Override
  public void beforeTestClass(final TestContext testContext) throws Exception {
    CouchbaseClient client = (CouchbaseClient) testContext.getApplicationContext().getBean("couchbaseClient");
    populateTestData(client);
    createAndWaitForDesignDocs(client);
  }

  private void populateTestData(CouchbaseClient client) {
    CouchbaseTemplate template = new CouchbaseTemplate(client);

    for(int i=0;i < 100; i++) {
      Beer b = new Beer("testbeer-" + i).setName("MyBeer " + i).setActive(true);
      template.save(b);
    }
  }

  private void createAndWaitForDesignDocs(CouchbaseClient client) {
    DesignDocument designDoc = new DesignDocument("test_beers");
    String mapFunction = "function (doc, meta) { if(doc._class == "
      + "\"org.springframework.data.couchbase.core.Beer\") { emit(doc.name, null); } }";
    designDoc.setView(new ViewDesign("by_name", mapFunction));
    client.createDesignDoc(designDoc);
  }

}
