/*
 * Copyright 2021-2024 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.util;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.data.couchbase.domain.Config;

import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.manager.collection.CollectionManager;

/**
 * Provides Collection support for integration tests
 *
 * @Author Michael Reiche
 */
public class CollectionAwareIntegrationTests extends JavaIntegrationTests {

	public static String scopeName = "my_scope";// + randomString();
	public static String otherScope = "other_scope";
	public static String collectionName = "my_collection";// + randomString();
	public static String collectionName2 = "my_collection2";// + randomString();
	public static String otherCollection = "other_collection";// + randomString();

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
		Cluster cluster = Cluster.connect(connectionString(),
				ClusterOptions.clusterOptions(authenticator()).environment(environment().build()));
		Bucket bucket = cluster.bucket(config().bucketname());
		bucket.waitUntilReady(Duration.ofSeconds(30));
		waitForService(bucket, ServiceType.QUERY);
		waitForQueryIndexerToHaveBucket(cluster, config().bucketname());
		CollectionManager collectionManager = bucket.collections();

		setupScopeCollection(cluster, scopeName, collectionName, collectionManager);
		setupScopeCollection(cluster, scopeName, collectionName2, collectionManager);

		if (otherScope != null || otherCollection != null) {
			// afterAll should be undoing the creation of scope etc
			setupScopeCollection(cluster, otherScope, otherCollection, collectionManager);
		}

		try {
			// needs an index for this N1ql Join
			// create index ix2 on my_bucket(parent_id) where `_class` = 'org.springframework.data.couchbase.domain.Address';

			List<String> fieldList = new ArrayList<>();
			fieldList.add("parentId");
			cluster.query("CREATE INDEX `parent_idx` ON default:`" + bucketName() + "`." + scopeName + "." + collectionName2
					+ "(parentId)");
		} catch (IndexExistsException ife) {
			LOGGER.warn("IndexFailureException occurred - ignoring: ", ife.toString());
		}
        logDisconnect(cluster, CollectionAwareIntegrationTests.class.getSimpleName());
		Config.setScopeName(scopeName);
        // ApplicationContext ac = new AnnotationConfigApplicationContext(Config.class);
        // System.out.println(ac);
		// the Config class has been modified, these need to be loaded again
        // couchbaseTemplate = (CouchbaseTemplate) ac.getBean(COUCHBASE_TEMPLATE);
        // reactiveCouchbaseTemplate = (ReactiveCouchbaseTemplate) ac.getBean(REACTIVE_COUCHBASE_TEMPLATE);
	}

	@AfterAll
	public static void afterAll() {
		Config.setScopeName(null);
        // ApplicationContext ac = new AnnotationConfigApplicationContext(Config.class);
		// the Config class has been modified, these need to be loaded again
        // couchbaseTemplate = (CouchbaseTemplate) ac.getBean(COUCHBASE_TEMPLATE);
        // reactiveCouchbaseTemplate = (ReactiveCouchbaseTemplate) ac.getBean(REACTIVE_COUCHBASE_TEMPLATE);
		callSuperAfterAll(new Object() {});
	}
}
