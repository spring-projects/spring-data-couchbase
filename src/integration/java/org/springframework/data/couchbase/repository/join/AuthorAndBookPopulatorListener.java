package org.springframework.data.couchbase.repository.join;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.cluster.ClusterInfo;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

/**
 * Populates author and book documents for N1ql Join tests
 *
 */
public class AuthorAndBookPopulatorListener extends DependencyInjectionTestExecutionListener {

    @Override
    public void beforeTestClass(final TestContext testContext) throws Exception {
        Bucket client = (Bucket) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_BUCKET);
        ClusterInfo clusterInfo = (ClusterInfo) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_CLUSTER_INFO);
        populateTestData(client, clusterInfo);
    }

    void populateTestData(Bucket client, ClusterInfo clusterInfo) {
        CouchbaseTemplate template = new CouchbaseTemplate(clusterInfo, client);
        for(int i=0;i<5;i++) {
            Author author = new Author("Author" + i,"foo"+ i);
            template.save(author);
            for (int j=0;j<5;j++) {
                Book book = new Book("Book" + i+j, "foo"+i, "");
                template.save(book);
            }
        }
    }

}