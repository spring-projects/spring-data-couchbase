package org.springframework.data.couchbase.repository.index;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;

import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

/**
 * A test listener that will remove the indexes created in {@link IndexedRepositoryIntegrationTests} before test case is run.
 *
 * @author Simon Baslé
 */
public class IndexedRepositoryTestListener extends DependencyInjectionTestExecutionListener {

  @Override
  public void beforeTestClass(final TestContext testContext) throws Exception {
    Bucket bucket = (Bucket) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_BUCKET);
    Cluster cluster = (Cluster) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_CLUSTER);
    // TODO: how did this used to work?  Did the creation of the beans above somehow kick off the
    // index manager to build indexes (and now if doesn't)?
    //
    //cluster.queryIndexes().dropPrimaryIndex(bucket.name());
    //cluster.queryIndexes().dropIndex(bucket.name(), IndexedRepositoryIntegrationTests.SECONDARY);
  }
}
