/*
 * Copyright 2012-2021 the original author or authors
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

import static java.nio.charset.StandardCharsets.UTF_8;

import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class UnmanagedTestCluster extends TestCluster {

	private final OkHttpClient httpClient = new OkHttpClient.Builder().build();
	private final String seedHost;
	private final int seedPort;
	private final String adminUsername;
	private final String adminPassword;
	private final int numReplicas;
	private volatile String bucketname;
	private long startTime = System.currentTimeMillis();

	UnmanagedTestCluster(final Properties properties) {
		seedHost = properties.getProperty("cluster.unmanaged.seed").split(":")[0];
		seedPort = Integer.parseInt(properties.getProperty("cluster.unmanaged.seed").split(":")[1]);
		adminUsername = properties.getProperty("cluster.adminUsername");
		adminPassword = properties.getProperty("cluster.adminPassword");
		numReplicas = Integer.parseInt(properties.getProperty("cluster.unmanaged.numReplicas"));
	}

	@Override
	ClusterType type() {
		return ClusterType.UNMANAGED;
	}

	@Override
	TestClusterConfig _start() throws Exception {
		bucketname = "my_bucket"; // UUID.randomUUID().toString();

		Response postResponse = httpClient
				.newCall(new Request.Builder().header("Authorization", Credentials.basic(adminUsername, adminPassword))
						.url("http://" + seedHost + ":" + seedPort + "/pools/default/buckets")
						.post(new FormBody.Builder().add("name", bucketname).add("bucketType", "membase").add("ramQuotaMB", "100")
								.add("replicaNumber", Integer.toString(numReplicas)).add("flushEnabled", "1").build())
						.build())
				.execute();

		String reason = postResponse.body().string();
		if (postResponse.code() != 202 && !(reason.contains("Bucket with given name already exists"))) {
			throw new Exception("Could not create bucket: " + postResponse + ", Reason: " + reason);
		}

		Response getResponse = httpClient
				.newCall(new Request.Builder().header("Authorization", Credentials.basic(adminUsername, adminPassword))
						.url("http://" + seedHost + ":" + seedPort + "/pools/default/b/" + bucketname).build())
				.execute();

		String raw = getResponse.body().string();

		waitUntilAllNodesHealthy();

		Optional<X509Certificate> cert = loadClusterCertificate();

		return new TestClusterConfig(bucketname, adminUsername, adminPassword, nodesFromRaw(seedHost, raw),
				replicasFromRaw(raw), cert, capabilitiesFromRaw(raw));
	}

	private Optional<X509Certificate> loadClusterCertificate() {
		try {
			Response getResponse = httpClient
					.newCall(new Request.Builder().header("Authorization", Credentials.basic(adminUsername, adminPassword))
							.url("http://" + seedHost + ":" + seedPort + "/pools/default/certificate").build())
					.execute();

			String raw = getResponse.body().string();

			CertificateFactory cf = CertificateFactory.getInstance("X.509");
			Certificate cert = cf.generateCertificate(new ByteArrayInputStream(raw.getBytes(UTF_8)));
			return Optional.of((X509Certificate) cert);
		} catch (Exception ex) {
			// could not load certificate, maybe add logging? could be CE instance.
			return Optional.empty();
		}
	}

	private void waitUntilAllNodesHealthy() throws Exception {
		while (true) {
			Response getResponse = httpClient
					.newCall(new Request.Builder().header("Authorization", Credentials.basic(adminUsername, adminPassword))
							.url("http://" + seedHost + ":" + seedPort + "/pools/default/").build())
					.execute();

			String raw = getResponse.body().string();

			Map<String, Object> decoded;
			try {
				decoded = (Map<String, Object>) MAPPER.readValue(raw, Map.class);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			List<Map<String, Object>> nodes = (List<Map<String, Object>>) decoded.get("nodes");
			int healthy = 0;
			for (Map<String, Object> node : nodes) {
				String status = (String) node.get("status");
				if (status.equals("healthy")) {
					healthy++;
				}
			}
			if (healthy == nodes.size()) {
				break;
			}
			Thread.sleep(100);
		}
	}

	@Override
	public void close() {
		try {
			if (!bucketname.equals("my_bucket")) {
				httpClient
						.newCall(new Request.Builder().header("Authorization", Credentials.basic(adminUsername, adminPassword))
								.url("http://" + seedHost + ":" + seedPort + "/pools/default/buckets/" + bucketname).delete().build())
						.execute();
			}
			System.out.println("elapsed: " + (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
}
