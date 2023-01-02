/*
 * Copyright 2017-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.env.ClusterEnvironment;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.query.QueryIndex;

@SpringJUnitConfig(CouchbaseRepositoryAutoQueryIndexIntegrationTests.Config.class)
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
public class CouchbaseRepositoryAutoQueryIndexIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired private Cluster cluster;

	/**
	 * Since the index creation happens at startup, the only way to properly check is by querying the index list and
	 * making sure it is present.
	 */
	@Test
	void createsSingleFieldIndex() {
		// This failed once against Capella.  Not sure why.
		Optional<QueryIndex> foundIndex = cluster.queryIndexes().getAllIndexes(bucketName()).stream()
				.filter(i -> i.name().equals("idx_airline_name")).findFirst();

		assertTrue(foundIndex.isPresent());
		assertTrue(foundIndex.get().condition().get().contains("_class"));
	}

	@Test
	void createsCompositeIndex() {
		Optional<QueryIndex> foundIndex = cluster.queryIndexes().getAllIndexes(bucketName()).stream()
				.filter(i -> i.name().equals("idx_airline_id_name")).findFirst();

		assertTrue(foundIndex.isPresent());
		assertTrue(foundIndex.get().condition().get().contains("_class"));
	}

	@Test
	void createsCompositeIndexWithPath() {
		Optional<QueryIndex> foundIndex = cluster.queryIndexes().getAllIndexes(bucketName()).stream()
				.filter(i -> i.name().equals("idx_airline_id_something_name")).findFirst();

		assertTrue(foundIndex.isPresent());
		assertTrue(foundIndex.get().condition().get().contains("_class"));
	}
	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	static class Config extends AbstractCouchbaseConfiguration {

		@Override
		public String getConnectionString() {
			return connectionString();
		}

		@Override
		public String getUserName() {
			return config().adminUsername();
		}

		@Override
		public String getPassword() {
			return config().adminPassword();
		}

		@Override
		public String getBucketName() {
			return bucketName();
		}

		@Override
		protected void configureEnvironment(ClusterEnvironment.Builder builder) {
			if(config().isUsingCloud()) {
				builder.securityConfig(SecurityConfig.builder()
						.trustManagerFactory(InsecureTrustManagerFactory.INSTANCE)
						.enableTls(true));
			}
		}
		@Override
		protected boolean autoIndexCreation() {
			return true;
		}
	}

}
