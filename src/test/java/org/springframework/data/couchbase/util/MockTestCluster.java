/*
 * Copyright 2012-2023 the original author or authors
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import com.couchbase.mock.Bucket;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.memcached.MemcachedServer;
import com.couchbase.mock.security.sasl.ShaSaslServerFactory;

/**
 * Implements the integration test cluster on top of the CouchbaseMock library.
 *
 * @since 2.0.0
 */
public class MockTestCluster extends TestCluster {

	private static final int RANDOM_PORT = 0;

	private final Properties properties;

	private volatile CouchbaseMock mock;

	MockTestCluster(Properties properties) {
		this.properties = properties;
	}

	@Override
	ClusterType type() {
		return ClusterType.MOCKED;
	}

	@Override
	TestClusterConfig _start() throws Exception {
		BucketConfiguration bucketConfig = new BucketConfiguration();
		bucketConfig.type = Bucket.BucketType.COUCHBASE;
		bucketConfig.numVBuckets = 1024;
		bucketConfig.numNodes = Integer.parseInt(properties.getProperty("cluster.mocked.numNodes"));
		bucketConfig.numReplicas = Integer.parseInt(properties.getProperty("cluster.mocked.numReplicas"));

		bucketConfig.name = UUID.randomUUID().toString();
		bucketConfig.password = UUID.randomUUID().toString();

		try {
			mock = new CouchbaseMock(RANDOM_PORT, Collections.singletonList(bucketConfig));
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

		mock.start();
		mock.waitForStartup();

		for (Bucket bucket : mock.getBuckets().values()) {
			for (MemcachedServer server : bucket.getServers()) {
				server.setCccpEnabled(true);

				List<String> mechs = new ArrayList<>(Arrays.asList(ShaSaslServerFactory.SUPPORTED_MECHS));
				mechs.add("PLAIN");
				server.setSaslMechanisms(mechs);
			}
		}

		List<TestNodeConfig> nodeConfigs = new ArrayList<>();
		for (Bucket bucket : mock.getBuckets().values()) {
			for (MemcachedServer server : bucket.getServers()) {
				Map<Services, Integer> ports = new HashMap<>();
				ports.put(Services.KV, server.getPort());
				ports.put(Services.MANAGER, mock.getHttpPort());
				ports.put(Services.VIEW, mock.getHttpPort()); // mock has views on the same http port

				nodeConfigs.add(new TestNodeConfig(server.getHostname(), ports));
			}
		}

		return new TestClusterConfig(bucketConfig.name, bucketConfig.name, bucketConfig.password, nodeConfigs,
				bucketConfig.numReplicas, Optional.empty(), // mock does not support certs
				EnumSet.of(Capabilities.VIEWS), // mock only has a limited set of capabilities we can utilize
				null);
	}

	@Override
	public void close() {
		if (mock != null) {
			mock.stop();
		}
	}
}
