/*
 * Copyright 2012-2020 the original author or authors
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

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;

/**
 * Parent class which drives all dynamic integration tests based on the configured cluster setup.
 *
 * @since 2.0.0
 */
@ExtendWith(ClusterInvocationProvider.class)
public abstract class ClusterAwareIntegrationTests {

	private static TestClusterConfig testClusterConfig;

	@BeforeAll
	static void setup(TestClusterConfig config) {
		testClusterConfig = config;
	}

	/**
	 * Returns the current config for the integration test cluster.
	 */
	public static TestClusterConfig config() {
		return testClusterConfig;
	}

	public static Authenticator authenticator() {
		return PasswordAuthenticator.create(config().adminUsername(), config().adminPassword());
	}

	public static String bucketName() {
		return config().bucketname();
	}

	/**
	 * Creates the right connection string out of the seed nodes in the config.
	 *
	 * @return the connection string to connect.
	 */
	public static String connectionString() {
		return seedNodes().stream().map(s -> {
			if (s.kvPort().isPresent()) {
				return s.address() + ":" + s.kvPort().get();
			} else {
				return s.address();
			}
		}).collect(Collectors.joining(","));
	}

	public static Set<SeedNode> seedNodes() {
		return config().nodes().stream().map(cfg -> SeedNode.create(cfg.hostname(),
				Optional.ofNullable(cfg.ports().get(Services.KV)), Optional.ofNullable(cfg.ports().get(Services.MANAGER))))
				.collect(Collectors.toSet());
	}

}
