package org.springframework.data.couchbase.repository;

import java.util.Collections;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.View;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.RxJavaCouchbaseTemplate;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

/**
 * @author Subhashni Balakrishnan
 */
public class SimpleReactiveCouchbaseRepositoryListener extends DependencyInjectionTestExecutionListener {

	@Override
	public void beforeTestClass(final TestContext testContext) throws Exception {
		Bucket client = (Bucket) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_BUCKET);
		ClusterInfo clusterInfo = (ClusterInfo) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_CLUSTER_INFO);
		populateTestData(client, clusterInfo);
		createAndWaitForDesignDocs(client);
	}

	private void populateTestData(Bucket client, ClusterInfo clusterInfo) {
		RxJavaCouchbaseTemplate template = new RxJavaCouchbaseTemplate(clusterInfo, client);

		for (int i = 0; i < 100; i++) {
			ReactiveUser u = new ReactiveUser("reactivetestuser-" + i, "reactiveuname-" + i, i);
			template.save(u, PersistTo.MASTER, ReplicateTo.NONE).subscribe();
		}

	}

	private void createAndWaitForDesignDocs(Bucket client) {
		String mapFunction = "function (doc, meta) { if(doc._class == \"org.springframework.data.couchbase.repository." +
				"ReactiveUser\") { emit(null, null); } }";
		View view = DefaultView.create("all", mapFunction, "_count");
		List<View> views = Collections.singletonList(view);
		DesignDocument designDoc = DesignDocument.create("reactiveUser", views);
		client.bucketManager().upsertDesignDocument(designDoc);
	}

}
