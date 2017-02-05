/*
 * Copyright 2017 the original author or authors.
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

import java.util.Collections;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.query.Index;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.view.*;

import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import rx.Observable;

/**
 * @author Subhashni Balakrishnan
 */
public class RxCouchbaseTemplateQueryListener extends DependencyInjectionTestExecutionListener {

	@Override
	public void beforeTestClass(final TestContext testContext) throws Exception {
		Bucket client = (Bucket) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_BUCKET);
		ClusterInfo clusterInfo = (ClusterInfo) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_CLUSTER_INFO);
		populateTestData(client, clusterInfo);
		createAndWaitForDesignDocs(client);
		client.query(N1qlQuery.simple(Index.createPrimaryIndex().on(client.name())));
	}

	private void populateTestData(Bucket client, ClusterInfo clusterInfo) {
		RxJavaCouchbaseTemplate template = new RxJavaCouchbaseTemplate(clusterInfo, client);
		for (int i = 0; i < 100; i++) {
			ReactiveBeer b = new ReactiveBeer("testbeer-" + i, "MyBeer" + i, true, "");
			template.save(b).subscribe();
		}
	}

	private void createAndWaitForDesignDocs(Bucket client) {
		String mapFunction = "function (doc, meta) { if(doc._class == "
				+ "\"org.springframework.data.couchbase.core.ReactiveBeer\") { emit(doc.name, null); } }";
		View view = DefaultView.create("by_name", mapFunction);
		DesignDocument designDoc = DesignDocument.create("reactive_test_beers", Collections.singletonList(view));
		client.bucketManager().upsertDesignDocument(designDoc);
	}

	@Override
	public void afterTestClass(final TestContext testContext) throws Exception {
		Bucket client = (Bucket) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_BUCKET);
		ClusterInfo clusterInfo = (ClusterInfo) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_CLUSTER_INFO);
		RxJavaCouchbaseTemplate template = new RxJavaCouchbaseTemplate(clusterInfo, client);

		for (int i = 0; i < 100; i++) {
			ReactiveBeer b = new ReactiveBeer("testbeer-" + i, "MyBeer" + i, true, "");
			template.remove(b).subscribe();
		}

	}
}