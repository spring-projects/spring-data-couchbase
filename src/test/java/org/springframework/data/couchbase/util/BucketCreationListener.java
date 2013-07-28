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

package org.springframework.data.couchbase.util;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.clustermanager.BucketType;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import java.net.URI;
import java.util.Arrays;

/**
 * @author Michael Nitschinger
 */
public class BucketCreationListener  extends DependencyInjectionTestExecutionListener {

  private CouchbaseClient client;

  @Override
  public void beforeTestClass(final TestContext testContext) throws Exception {

    BucketManager bucketManager = (BucketManager) testContext.getApplicationContext().getBean("bucketManager");

    bucketManager.deleteAllBuckets();
    bucketManager.createDefaultBucket(BucketType.COUCHBASE, 256, 1, true);
    BucketManager.FunctionCallback callback = new BucketManager.FunctionCallback() {

      @Override
      public void callback() throws Exception {
        initTemplate(testContext);
      }

      @Override
      public String success(long elapsedTime) {
        return "Bucket clearance took " + elapsedTime + "ms";
      }
    };

    bucketManager.poll(callback);
    bucketManager.waitForWarmup(client);
  }


  protected void initTemplate(final TestContext testContext) throws Exception {
    String host = (String) testContext.getApplicationContext().getBean("couchbaseHost");
    String bucket = (String) testContext.getApplicationContext().getBean("couchbaseBucket");
    String password = (String) testContext.getApplicationContext().getBean("couchbasePassword");

    client = new CouchbaseClient(Arrays.asList(new URI(host)), bucket, password);
  }

}
