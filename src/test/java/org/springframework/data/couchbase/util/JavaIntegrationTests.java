/*
 * Copyright 2020 the original author or authors
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

import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static com.couchbase.client.core.util.CbThrowables.throwIfUnchecked;
import static com.couchbase.client.java.AsyncUtils.block;
import static com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.data.couchbase.config.BeanNames.COUCHBASE_TEMPLATE;
import static org.springframework.data.couchbase.config.BeanNames.REACTIVE_COUCHBASE_TEMPLATE;
import static org.springframework.data.couchbase.util.Util.waitUntilCondition;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;

import com.couchbase.client.core.diagnostics.PingResult;
import com.couchbase.client.core.diagnostics.PingState;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.ParsingFailureException;
import com.couchbase.client.core.error.QueryException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.diagnostics.PingOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.manager.search.UpsertSearchIndexOptions;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchResult;
import org.springframework.data.couchbase.domain.Config;

/**
 * Extends the {@link ClusterAwareIntegrationTests} with java-client specific code.
 *
 * @Author Michael Reiche
 */
// Temporarily increased timeout to (possibly) workaround MB-37011 when Developer Preview enabled
@Timeout(value = 10, unit = TimeUnit.MINUTES) // Safety timer so tests can't block CI executors
public class JavaIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired static public CouchbaseTemplate couchbaseTemplate;
	@Autowired static public ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
		try (CouchbaseClientFactory couchbaseClientFactory = new SimpleCouchbaseClientFactory(connectionString(),
				authenticator(), bucketName())) {
			couchbaseClientFactory.getCluster().queryIndexes().createPrimaryIndex(bucketName(),
					CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions().ignoreIfExists(true));
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
		ApplicationContext ac = new AnnotationConfigApplicationContext(Config.class);
		couchbaseTemplate = (CouchbaseTemplate) ac.getBean(COUCHBASE_TEMPLATE);
		reactiveCouchbaseTemplate = (ReactiveCouchbaseTemplate) ac.getBean(REACTIVE_COUCHBASE_TEMPLATE);
	}

	/**
	 * Creates a {@link ClusterEnvironment.Builder} which already has the seed nodes and credentials plugged and ready to
	 * use depending on the environment.
	 *
	 * @return the builder, ready to be further modified or used directly.
	 */
	protected static ClusterEnvironment.Builder environment() {
		return ClusterEnvironment.builder();
	}

	/**
	 * Returns the pre-set cluster options with the environment and authenticator configured.
	 *
	 * @return the cluster options ready to be used.
	 */
	protected static ClusterOptions clusterOptions() {
		return ClusterOptions.clusterOptions(authenticator()).environment(environment().build());
	}

	/**
	 * Helper method to create a primary index if it does not exist.
	 */
	protected static void createPrimaryIndex(final Cluster cluster, final String bucketName) {
		cluster.queryIndexes().createPrimaryIndex(bucketName, createPrimaryQueryIndexOptions().ignoreIfExists(true));
	}

	public static void setupScopeCollection(Cluster cluster, String scopeName, String collectionName,
			CollectionManager collectionManager) {
		// Create the scope.collection (borrowed from CollectionManagerIntegrationTest )
		ScopeSpec scopeSpec = ScopeSpec.create(scopeName);
		CollectionSpec collSpec = CollectionSpec.create(collectionName, scopeName);

		if (!scopeName.equals("_default")) {
			collectionManager.createScope(scopeName);
		}

		waitUntilCondition(() -> scopeExists(collectionManager, scopeName));
		ScopeSpec found = collectionManager.getScope(scopeName);
		assertEquals(scopeSpec, found);

		collectionManager.createCollection(collSpec);
		waitUntilCondition(() -> collectionExists(collectionManager, collSpec));
		waitUntilCondition(
				() -> collectionReady(cluster.bucket(config().bucketname()).scope(scopeName).collection(collectionName)));

		assertNotEquals(scopeSpec, collectionManager.getScope(scopeName));
		assertTrue(collectionManager.getScope(scopeName).collections().contains(collSpec));

		waitForQueryIndexerToHaveBucket(cluster, collectionName);

		// the call to createPrimaryIndex takes about 60 seconds

		try {
			block(createPrimaryIndex(cluster, config().bucketname(), scopeName, collectionName));
		} catch (Exception e) {
			e.printStackTrace();
		}

		waitUntilCondition(
				() -> collectionReadyQuery(cluster.bucket(config().bucketname()).scope(scopeName), collectionName));
	}

	protected static void waitForQueryIndexerToHaveBucket(final Cluster cluster, final String bucketName) {
		boolean ready = false;
		int guard = 100;

		while (!ready && guard != 0) {
			guard -= 1;
			String statement = "SELECT COUNT(*) > 0 as present FROM system:keyspaces where name = '" + bucketName + "';";

			QueryResult queryResult = cluster.query(statement);
			List<JsonObject> rows = queryResult.rowsAsObject();
			if (rows.size() == 1 && rows.get(0).getBoolean("present")) {
				ready = true;
			}

			if (!ready) {
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {}
			}
		}

		if (guard == 0) {
			throw new IllegalStateException("Query indexer is still not aware of bucket " + bucketName);
		}
	}

	/**
	 * Improve test stability by waiting for a given service to report itself ready.
	 */
	protected static void waitForService(final Bucket bucket, final ServiceType serviceType) {
		bucket.waitUntilReady(Duration.ofSeconds(30));

		Util.waitUntilCondition(() -> {
			PingResult pingResult = bucket.ping(PingOptions.pingOptions().serviceTypes(Collections.singleton(serviceType)));

			return pingResult.endpoints().containsKey(serviceType) && pingResult.endpoints().get(serviceType).size() > 0
					&& pingResult.endpoints().get(serviceType).get(0).state() == PingState.OK;
		});
	}

	public static boolean collectionExists(CollectionManager mgr, CollectionSpec spec) {
		try {
			ScopeSpec scope = mgr.getScope(spec.scopeName());
			return scope.collections().contains(spec);
		} catch (CollectionNotFoundException e) {
			return false;
		}
	}

	public static boolean collectionReady(Collection collection) {
		try {
			collection.get("123");
			return true;
		} catch (DocumentNotFoundException dnfe) {
			return true;
		} catch (UnambiguousTimeoutException e) {
			if (!e.toString().contains("COLLECTION_NOT_FOUND")) {
				throw e;
			}
			return false;
		}
	}

	public static boolean collectionReadyQuery(Scope scope, String collectionName) {
		try {
			scope.query("select * from `" + collectionName + "` where meta().id=\"1\"");
			return true;
		} catch (DocumentNotFoundException dnfe) {
			return true;
		} catch (ParsingFailureException e) {
			return false;
		}
	}

	public static boolean scopeExists(CollectionManager mgr, String scopeName) {
		try {
			mgr.getScope(scopeName);
			return true;
		} catch (ScopeNotFoundException e) {
			return false;
		}
	}

	public static CompletableFuture<Void> createPrimaryIndex(Cluster cluster, String bucketName, String scopeName,
			String collectionName) {
		CreatePrimaryQueryIndexOptions options = CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions();
		options.timeout(Duration.ofSeconds(300));
		final CreatePrimaryQueryIndexOptions.Built builtOpts = options.build();
		final String indexName = builtOpts.indexName().orElse(null);

		String keyspace = "default:`" + bucketName + "`.`" + scopeName + "`.`" + collectionName + "`";
		String statement = "CREATE PRIMARY INDEX ";
		if (indexName != null) {
			statement += (indexName) + " ";
		}
		statement += "ON " + (keyspace); // do not quote, this might be "default:bucketName.scopeName.collectionName"

		return exec(cluster, false, statement, builtOpts.with(), builtOpts).exceptionally(t -> {
			if (builtOpts.ignoreIfExists() && hasCause(t, IndexExistsException.class)) {
				return null;
			}
			throwIfUnchecked(t);
			throw new RuntimeException(t);
		}).thenApply(result -> null);
	}

	private static CompletableFuture<QueryResult> exec(Cluster cluster,
			/*AsyncQueryIndexManager.QueryType queryType*/ boolean queryType, CharSequence statement,
			Map<String, Object> with, CommonOptions<?>.BuiltCommonOptions options) {
		return with.isEmpty() ? exec(cluster, queryType, statement, options)
				: exec(cluster, queryType, statement + " WITH " + Mapper.encodeAsString(with), options);
	}

	private static CompletableFuture<QueryResult> exec(Cluster cluster,
			/*AsyncQueryIndexManager.QueryType queryType,*/ boolean queryType, CharSequence statement,
			CommonOptions<?>.BuiltCommonOptions options) {
		QueryOptions queryOpts = toQueryOptions(options).readonly(queryType /*requireNonNull(queryType) == READ_ONLY*/);

		return cluster.async().query(statement.toString(), queryOpts).exceptionally(t -> {
			throw translateException(t);
		});
	}

	private static QueryOptions toQueryOptions(CommonOptions<?>.BuiltCommonOptions options) {
		QueryOptions result = QueryOptions.queryOptions();
		options.timeout().ifPresent(result::timeout);
		options.retryStrategy().ifPresent(result::retryStrategy);
		return result;
	}

	private static final Map<Predicate<QueryException>, Function<QueryException, ? extends QueryException>> errorMessageMap = new LinkedHashMap<>();

	private static RuntimeException translateException(Throwable t) {
		if (t instanceof QueryException) {
			final QueryException e = ((QueryException) t);

			for (Map.Entry<Predicate<QueryException>, Function<QueryException, ? extends QueryException>> entry : errorMessageMap
					.entrySet()) {
				if (entry.getKey().test(e)) {
					return entry.getValue().apply(e);
				}
			}
		}
		return (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
	}

	public static void createFtsCollectionIndex(Cluster cluster, String indexName, String bucketName, String scopeName,
			String collectionName) {
		SearchIndex searchIndex = new SearchIndex(indexName, bucketName);
		if (scopeName != null) {
			// searchIndex = searchIndex.forScopeCollection(scopeName, collectionName);
			throw new RuntimeException("forScopeCollection not implemented in current java client version");
		}

		cluster.searchIndexes().upsertIndex(searchIndex,
				UpsertSearchIndexOptions.upsertSearchIndexOptions().timeout(Duration.ofSeconds(60)));

		int maxTries = 5;
		for (int i = 0; i < maxTries; i++) {
			try {
				SearchResult result = cluster.searchQuery(indexName, SearchQuery.queryString("junk"));
				break;
			} catch (CouchbaseException | IllegalStateException ex) {
				// this is a pretty dirty hack to avoid a race where we don't know if the index is ready yet
				System.out.println("createFtsCollectionIndex: " + i + " " + ex);
				if (i < (maxTries - 1) && (ex.getMessage().contains("no planPIndexes for indexName")
						|| ex.getMessage().contains("pindex_consistency mismatched partition")
						|| ex.getMessage().contains("pindex not available"))) {
					sleepMs(1000);
					continue;
				}
				throw ex;
			}
		}
	}

	public static String randomString() {
		return UUID.randomUUID().toString().substring(0, 8);
	}

	public static void sleepMs(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException ie) {}
	}
}
