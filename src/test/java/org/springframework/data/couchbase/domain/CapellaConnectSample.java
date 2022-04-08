/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.couchbase.domain;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static java.nio.charset.StandardCharsets.UTF_8;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.tls.HandshakeCertificates;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.query.QueryResult;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Sample code for connecting to Capella through both the control-plane and the data-plane. An Access Key and a Secret
 * Key are required and a bucket named "my_bucket" on the 'last' cluster.
 */
public class CapellaConnectSample {

	static final String cbc_access_key = "3gcpgyTBzOetdETYxOAtmLYBe3f9ZSVN";
	static final String cbc_secret_key = "PWiACuJIZUlv0fCZaIQbhI44NDXVZCDdRBbpdaWlACioN7jkuOINCUVrU2QL1jVO";
	static final String hostname = "cloudapi.cloud.couchbase.com";
	static final HandshakeCertificates clientCertificates = new HandshakeCertificates.Builder()
			.addPlatformTrustedCertificates().addInsecureHost(hostname).build();
	static final OkHttpClient httpClient = new OkHttpClient.Builder()
			.sslSocketFactory(clientCertificates.sslSocketFactory(), clientCertificates.trustManager()).build();

	protected static final ObjectMapper MAPPER = new ObjectMapper();
	static final String authorizationHeaderLabel = "Authorization";
	static final String timestampHeaderLabel = "Couchbase-Timestamp";

	public static void main(String... args) {
		String endpoint = null; // "cb.zsibzkbgllfbcj8g.cloud.couchbase.com";
		List<String> clusterIds = getClustersControlPlane();
		for (String id : clusterIds) {
			endpoint = getClusterControlPlane(id);
		}

		// Update this to your cluster
		String bucketName = "my_bucket";
		String username = "user";
		String password = "Couch0base!";
		// User Input ends here.

		ClusterEnvironment env = ClusterEnvironment.builder()
				.securityConfig(SecurityConfig.enableTls(true).trustManagerFactory(InsecureTrustManagerFactory.INSTANCE))
				.ioConfig(IoConfig.enableDnsSrv(true)).build();

		// Initialize the Connection
		Cluster cluster = Cluster.connect(endpoint, ClusterOptions.clusterOptions(username, password).environment(env));
		Bucket bucket = cluster.bucket(bucketName);
		bucket.waitUntilReady(Duration.parse("PT10S"));
		Collection collection = bucket.defaultCollection();

		cluster.queryIndexes().createPrimaryIndex(bucketName,
				CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions().ignoreIfExists(true));

		// Create a JSON Document
		JsonObject arthur = JsonObject.create().put("name", "Arthur").put("email", "kingarthur@couchbase.com")
				.put("interests", JsonArray.from("Holy Grail", "African Swallows"));

		// Store the Document
		collection.upsert("u:king_arthur", arthur);

		// Load the Document and print it
		// Prints Content and Metadata of the stored Document
		System.err.println(collection.get("u:king_arthur"));

		// Perform a N1QL Query
		QueryResult result = cluster.query(String.format("SELECT name FROM `%s` WHERE $1 IN interests", bucketName),
				queryOptions().parameters(JsonArray.from("African Swallows")));

		// Print each found Row
		for (JsonObject row : result.rowsAsObject()) {
			System.err.println(row);
		}

		cluster.disconnect();
	}

	public static List<String> getClustersControlPlane() {
		List<String> clusterIds = new ArrayList<>();
		Map<String, Object> decoded = doRequest(hostname, "GET", "/v3/clusters");
		HashMap data = (HashMap) decoded.get("data");
		List<Map> items = (List<Map>) data.get("items");
		for (Map m : items) {
			clusterIds.add((String) m.get("id"));
		}
		return clusterIds;
	}

	public static String getClusterControlPlane(String clusterId) {
		String endpointsSrv;
		Map<String, Object> decoded = doRequest(hostname, "GET", "/v3/clusters/" + clusterId);
		endpointsSrv = (String) decoded.get("endpointsSrv");
		return endpointsSrv;
	}

	private static Map<String, Object> doRequest(String hostname, String cbc_api_method, String cbc_api_endpoint) {
		Map<String, Object> decoded;
		String responseString;
		try {
			String cbc_api_now = Long.toString(System.currentTimeMillis());
			String authorizationValue = getApiSignature(cbc_api_method, cbc_api_endpoint, cbc_api_now);
			String urlString = "https://" + hostname + cbc_api_endpoint;
			System.err.println("curl --header \"" + authorizationHeaderLabel + ": " + authorizationValue + "\" --header \""
					+ timestampHeaderLabel + ": " + cbc_api_now + "\" " + urlString);
			Response response = httpClient.newCall(new Request.Builder().header(authorizationHeaderLabel, authorizationValue)
					.header(timestampHeaderLabel, cbc_api_now).url(urlString).build()).execute();
			responseString = response.body().string();
			System.err.println(responseString);
		} catch (IOException | NoSuchAlgorithmException | InvalidKeyException e) {
			throw new RuntimeException(e);
		}

		try {
			decoded = (Map<String, Object>) MAPPER.readValue(responseString.getBytes(UTF_8), Map.class);
		} catch (IOException e) {
			throw new RuntimeException("Error decoding, raw: " + responseString, e);
		}
		return decoded;
	}

	private static String getApiSignature(String cbc_api_method, String cbc_api_endpoint, String cbc_api_now)
			throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {
		String cbc_api_message = cbc_api_method + '\n' + cbc_api_endpoint + '\n' + cbc_api_now;
		return "Bearer " + cbc_access_key + ':' + new String(Base64.getEncoder()
				.encode(hmac("hmacSHA256", cbc_secret_key.getBytes("utf-8"), cbc_api_message.getBytes("utf-8"))));
	}

	static byte[] hmac(String algorithm, byte[] key, byte[] message)
			throws NoSuchAlgorithmException, InvalidKeyException {
		Mac mac = Mac.getInstance(algorithm);
		mac.init(new SecretKeySpec(key, algorithm));
		return mac.doFinal(message);
	}

}
