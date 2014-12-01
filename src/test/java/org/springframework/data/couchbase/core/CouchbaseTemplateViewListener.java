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

import java.util.Arrays;

import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.View;

/**
 * @author Michael Nitschinger
 */
public class CouchbaseTemplateViewListener extends DependencyInjectionTestExecutionListener {

  @Override
  public void beforeTestClass(final TestContext testContext) throws Exception {
    Bucket client =
        (Bucket) testContext.getApplicationContext().getBean("couchbaseClient");
    populateTestData(client);
    createAndWaitForDesignDocs(client.bucketManager());
  }

  private void populateTestData(Bucket client) {
    CouchbaseTemplate template = new CouchbaseTemplate(client);

    for(int i=0;i < 100; i++) {
      Beer b = new Beer("testbeer-" + i).setName("MyBeer " + i).setActive(true);
      template.save(b);
    }
  }

  private void createAndWaitForDesignDocs(BucketManager manager) {
    final String mapFunction =
        "function (doc, meta) { if(doc._class == "
        + "\"org.springframework.data.couchbase.core.Beer\") { emit(doc.name, null); } }";
    final View view = DefaultView.create("by_name", mapFunction);
    final DesignDocument designDoc = DesignDocument.create("test_beers", Arrays.asList(view));
    manager.upsertDesignDocument(designDoc);
  }

}
