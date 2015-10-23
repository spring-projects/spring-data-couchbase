package org.springframework.data.couchbase.repository.index;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.query.Index;
import com.couchbase.client.java.query.N1qlQuery;

import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

/**
 * A test listener that will remove the indexes created in {@link IndexedRepositoryTests} before test case is run.
 *
 * @author Simon Baslé
 */
public class IndexedRepositoryTestListener extends DependencyInjectionTestExecutionListener {

  @Override
  public void beforeTestClass(final TestContext testContext) throws Exception {
    Bucket client = (Bucket) testContext.getApplicationContext().getBean("couchbaseBucket");
    client.bucketManager().removeDesignDocument(IndexedRepositoryTests.VIEW_DOC);
    client.query(N1qlQuery.simple(Index.dropPrimaryIndex(client.name())));
    client.query(N1qlQuery.simple(Index.dropIndex(client.name(), IndexedRepositoryTests.SECONDARY)));
  }
}
