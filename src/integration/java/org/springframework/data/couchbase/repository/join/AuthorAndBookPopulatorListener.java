/*
 * Copyright 2012-2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * @author Tayeb Chlyah
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
            Address address = new Address("Address" + i, "foo" + i, "bar");
            template.save(address);
        }
    }

}