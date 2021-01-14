/*
 * Copyright 2021 the original author or authors
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

import static org.springframework.data.couchbase.config.BeanNames.COUCHBASE_TEMPLATE;
import static org.springframework.data.couchbase.config.BeanNames.REACTIVE_COUCHBASE_TEMPLATE;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.domain.Config;

import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.collection.CollectionManager;
import org.springframework.data.couchbase.domain.ConfigScoped;

/**
 * Provides Collection support for integration tests
 *
 * @Author Michael Reiche
 */
public class CollectionAwareIntegrationTests extends JavaIntegrationTests {

	public static String scopeName = "scope_" + randomString();
	public static String collectionName = "collection_" + randomString();

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
		ClusterEnvironment environment = environment().build();
		Cluster cluster = Cluster.connect(seedNodes(),
				ClusterOptions.clusterOptions(authenticator()).environment(environment));
		Bucket bucket = cluster.bucket(config().bucketname());
		bucket.waitUntilReady(Duration.ofSeconds(5));
		waitForService(bucket, ServiceType.QUERY);
		waitForQueryIndexerToHaveBucket(cluster, config().bucketname());
		CollectionManager collectionManager = bucket.collections();
		if (scopeName != null || collectionName != null) {
			setupScopeCollection(cluster, scopeName, collectionName, collectionManager);
		}

		ConfigScoped.setScopeName(scopeName);
		ApplicationContext ac = new AnnotationConfigApplicationContext(ConfigScoped.class);
		couchbaseTemplate = (CouchbaseTemplate) ac.getBean(COUCHBASE_TEMPLATE);
		reactiveCouchbaseTemplate = (ReactiveCouchbaseTemplate) ac.getBean(REACTIVE_COUCHBASE_TEMPLATE);
	}

	@AfterAll
	public static void afterAll(){
		System.out.println("CollectionAwareIntegrationTests.afterAll()");
		ConfigScoped.setScopeName(null);
		callSuperBeforeAll(new Object() {});
	}
}
