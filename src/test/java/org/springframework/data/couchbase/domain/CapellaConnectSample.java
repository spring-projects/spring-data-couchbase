/*
 * Copyright 2022-2023 the original author or authors.
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
 * Key are required and a bucket named "my_bucket" on the 'last' cluster. <br>
 * 1) Create a cluster that has data, index and query nodes. <br>
 * 2) Cluster -> Connectivity : allow your client ip address (or all ip address 0/0.0.0.0)<br>
 * 3) Create a user "user" in the cluster with password "Couch0base!" and Read/Write access to all buckets <br>
 * 4) Create a bucket named "my_bucket" <br>
 * 5) Get your access key from API Keys. The secret key is available only when the key is generated. If you have not
 * saved it, then generate a new key and save the secret key. <br>
 */
public class CapellaConnectSample {

	static final String cbc_access_key = "3gcpgyTBzOetdETYxOAtmLYBe3f9ZSVN"; // replace with your access key and...
	static final String cbc_secret_key = "PWiACuJIZUlv0fCZaIQbhI44NDXVZCDdRBbpdaWlACioN7jkuOINCUVrU2QL1jVO"; // secret key
	// Update this to your cluster
	static String bucketName = "my_bucket";
	static String username = "user";
	static String password = "Couch0base!";
	// User Input ends here.

	static final String hostname = "cloudapi.cloud.couchbase.com";
	static final HandshakeCertificates clientCertificates = new HandshakeCertificates.Builder()
			.addPlatformTrustedCertificates()/*.addInsecureHost(hostname)*/.build();
	static final OkHttpClient httpClient = new OkHttpClient.Builder()
			.sslSocketFactory(clientCertificates.sslSocketFactory(), clientCertificates.trustManager()).build();

	protected static final ObjectMapper MAPPER = new ObjectMapper();
	static final String authorizationHeaderLabel = "Authorization";
	static final String timestampHeaderLabel = "Couchbase-Timestamp";

	public static void main(String... args) {
		String endpoint = null; // "cb.zsibzkbgllfbcj8g.cloud.couchbase.com";
		List<String> clusterIds = getClustersControlPlane();
		// the following loop assumes that the desired cluster is the last one in the list.
		// If this is not the case, then the endpoint for the desired cluster must be selected.
		for (String id : clusterIds) {
			endpoint = getClusterControlPlane(id);
		}

		ClusterEnvironment env = ClusterEnvironment.builder()
				.securityConfig(SecurityConfig.enableTls(true)/*.trustManagerFactory(InsecureTrustManagerFactory.INSTANCE)*/)
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

	// the methods below are required only to get the endpoint (host)

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
