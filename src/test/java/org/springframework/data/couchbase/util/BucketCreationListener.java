package org.springframework.data.couchbase.util;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.clustermanager.BucketType;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import java.net.URI;
import java.util.Arrays;


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
