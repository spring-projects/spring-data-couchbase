/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.diagnostics.DiagnosticsResult;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.diagnostics.PingResult;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.msg.search.SearchRequest;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.diagnostics.DiagnosticsOptions;
import com.couchbase.client.java.diagnostics.PingOptions;
import com.couchbase.client.java.diagnostics.WaitUntilReadyOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.analytics.AnalyticsIndexManager;
import com.couchbase.client.java.manager.bucket.BucketManager;
import com.couchbase.client.java.manager.eventing.EventingFunctionManager;
import com.couchbase.client.java.manager.query.QueryIndexManager;
import com.couchbase.client.java.manager.search.SearchIndexManager;
import com.couchbase.client.java.manager.user.UserManager;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.transactions.Transactions;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.AsyncCluster.extractClusterEnvironment;
import static com.couchbase.client.java.AsyncCluster.seedNodesFromConnectionString;
import static com.couchbase.client.java.AsyncUtils.block;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_ANALYTICS_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_DIAGNOSTICS_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_QUERY_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_SEARCH_OPTIONS;

/**
 * The {@link Cluster} is the main entry point when connecting to a Couchbase cluster.
 * <p>
 * Most likely you want to start out by using the {@link #connect(String, String, String)} entry point. For more
 * advanced options you want to use the {@link #connect(String, ClusterOptions)} method. The entry point that allows
 * overriding the seed nodes ({@link #connect(Set, ClusterOptions)} is only needed if you run a couchbase cluster
 * at non-standard ports.
 * <p>
 * See the individual connect methods for more information, but here is a snippet to get you off the ground quickly. It
 * assumes you have Couchbase running locally and the "travel-sample" sample bucket loaded:
 * <pre>
 * //Connect and open a bucket
 * Cluster cluster = Cluster.connect("127.0.0.1","Administrator","password");
 * Bucket bucket = cluster.bucket("travel-sample");
 * Collection collection = bucket.defaultCollection();
 *
 * // Perform a N1QL query
 * QueryResult queryResult = cluster.query("select * from `travel-sample` limit 5");
 * System.out.println(queryResult.rowsAsObject());
 *
 * // Perform a KV request and load a document
 * GetResult getResult = collection.get("airline_10");
 * System.out.println(getResult);
 * </pre>
 * <p>
 * When the application shuts down (or the SDK is not needed anymore), you are required to call {@link #disconnect()}.
 * If you omit this step, the application will terminate (all spawned threads are daemon threads) but any operations
 * or work in-flight will not be able to complete and lead to undesired side-effects. Note that disconnect will also
 * shutdown all associated {@link Bucket buckets}.
 * <p>
 * Cluster-level operations like {@link #query(String)} will not work unless at leas one bucket is opened against a
 * pre 6.5 cluster. If you are using 6.5 or later, you can run cluster-level queries without opening a bucket. All
 * of these operations are lazy, so the SDK will bootstrap in the background and service queries as quickly as possible.
 * This also means that the first operations might be a bit slower until all sockets are opened in the background and
 * the configuration is loaded. If you want to wait explicitly, you can utilize the {@link #waitUntilReady(Duration)}
 * method before performing your first query.
 * <p>
 * The SDK will only work against Couchbase Server 5.0 and later, because RBAC (role-based access control) is a first
 * class concept since 3.0 and therefore required.
 */
// todo gpx is this required?
public class Cluster implements ClusterInterface {

  /**
   * Holds the underlying async cluster reference.
   */
  private final AsyncCluster asyncCluster;

  /**
   * Holds the adjacent reactive cluster reference.
   */
  private final ReactiveCluster reactiveCluster;

  /**
   * The search index manager manages search indexes.
   */
  private final SearchIndexManager searchIndexManager;

  /**
   * The user manager manages users and groups.
   */
  private final UserManager userManager;

  /**
   * The bucket manager manages buckets and allows to flush them.
   */
  private final BucketManager bucketManager;

  /**
   * Allows to manage query indexes.
   */
  private final QueryIndexManager queryIndexManager;

  /**
   * Allows to manage analytics indexes.
   */
  private final AnalyticsIndexManager analyticsIndexManager;

  /**
   * Allows to manage eventing functions.
   */
  private final EventingFunctionManager eventingFunctionManager;

  /**
   * Stores already opened buckets for reuse.
   */
  private final Map<String, Bucket> bucketCache = new ConcurrentHashMap<>();

  /**
   * Connect to a Couchbase cluster with a username and a password as credentials.
   * <p>
   * This is the simplest (and recommended) method to connect to the cluster if you do not need to provide any
   * custom options.
   * <p>
   * The first argument (the connection string in its simplest form) is used to supply the hostnames of the cluster. In
   * development it is OK to only pass in one hostname (or IP address), but in production we recommend passing in at
   * least 3 nodes of the cluster (comma separated). The reason is that if one or more of the nodes are not reachable
   * the client will still be able to bootstrap (and your application will become more resilient as a result).
   * <p>
   * Here is how you specify one node to use for bootstrapping:
   * <pre>
   * Cluster cluster = Cluster.connect("127.0.0.1", "user", "password"); // ok during development
   * </pre>
   * This is what we recommend in production:
   * <pre>
   * Cluster cluster = Cluster.connect("host1,host2,host3", "user", "password"); // recommended in production
   * </pre>
   * It is important to understand that the SDK will only use the bootstrap ("seed nodes") host list to establish an
   * initial contact with the cluster. Once the configuration is loaded this list is discarded and the client will
   * connect to all nodes based on this configuration.
   * <p>
   * This method will return immediately and the SDK will try to establish all the necessary resources and connections
   * in the background. This means that depending on how fast it can be bootstrapped, the first couple cluster-level
   * operations like {@link #query(String)} will take a bit longer. If you want to wait explicitly until those resources
   * are available, you can use the {@link #waitUntilReady(Duration)} method before running any of them:
   * <pre>
   * Cluster cluster = Cluster.connect("host1,host2,host3", "user", "password");
   * cluster.waitUntilReady(Duration.ofSeconds(5));
   * QueryResult result = cluster.query("select * from bucket limit 1");
   * </pre>
   *
   * @param connectionString connection string used to locate the Couchbase cluster.
   * @param username the name of the user with appropriate permissions on the cluster.
   * @param password the password of the user with appropriate permissions on the cluster.
   * @return the instantiated {@link Cluster}.
   */
  public static Cluster connect(final String connectionString, final String username, final String password) {
    return connect(connectionString, clusterOptions(PasswordAuthenticator.create(username, password)));
  }

  /**
   * Connect to a Couchbase cluster with custom options.
   * <p>
   * You likely want to use this over the simpler {@link #connect(String, String, String)} if:
   * <ul>
   *   <li>A custom {@link ClusterEnvironment}</li>
   *   <li>Or a custom {@link Authenticator}</li>
   * </ul>
   * needs to be provided.
   * <p>
   * A custom environment can be passed in like this:
   * <pre>
   * // on bootstrap:
   * ClusterEnvironment environment = ClusterEnvironment.builder().build();
   * Cluster cluster = Cluster.connect(
   *   "127.0.0.1",
   *   clusterOptions("user", "password").environment(environment)
   * );
   *
   * // on shutdown:
   * cluster.disconnect();
   * environment.shutdown();
   * </pre>
   * It is <strong>VERY</strong> important to shut down the environment when being passed in separately (as shown in
   * the code sample above) and <strong>AFTER</strong> the cluster is disconnected. This will ensure an orderly shutdown
   * and makes sure that no resources are left lingering.
   * <p>
   * If you want to pass in a custom {@link Authenticator}, it is likely because you are setting up certificate-based
   * authentication instead of using a username and a password directly. Remember to also enable TLS.
   * <pre>
   * ClusterEnvironment environment = ClusterEnvironment
   *   .builder()
   *   .securityConfig(SecurityConfig.enableTls(true))
   *   .build();
   *
   * Authenticator authenticator = CertificateAuthenticator.fromKey(...);
   *
   * Cluster cluster = Cluster.connect(
   *   "127.0.0.1",
   *   clusterOptions(authenticator).environment(environment)
   * );
   * </pre>
   * This method will return immediately and the SDK will try to establish all the necessary resources and connections
   * in the background. This means that depending on how fast it can be bootstrapped, the first couple cluster-level
   * operations like {@link #query(String)} will take a bit longer. If you want to wait explicitly until those resources
   * are available, you can use the {@link #waitUntilReady(Duration)} method before running any of them:
   * <pre>
   * Cluster cluster = Cluster.connect("host1,host2,host3", "user", "password");
   * cluster.waitUntilReady(Duration.ofSeconds(5));
   * QueryResult result = cluster.query("select * from bucket limit 1");
   * </pre>
   *
   * @param connectionString connection string used to locate the Couchbase cluster.
   * @param options custom options when creating the cluster.
   * @return the instantiated {@link Cluster}.
   */
  public static Cluster connect(final String connectionString, final ClusterOptions options) {
    notNullOrEmpty(connectionString, "ConnectionString");
    notNull(options, "ClusterOptions");

    final ClusterOptions.Built opts = options.build();
    final Supplier<ClusterEnvironment> environmentSupplier = extractClusterEnvironment(connectionString, opts);
    return new Cluster(
      environmentSupplier,
      opts.authenticator(),
      seedNodesFromConnectionString(connectionString, environmentSupplier.get())
    );
  }

  /**
   * Connect to a Couchbase cluster with a list of seed nodes and custom options.
   * <p>
   * Note that you likely only want to use this method if you need to pass in custom ports for specific seed nodes
   * during bootstrap. Otherwise we recommend relying on the simpler {@link #connect(String, String, String)} method
   * instead.
   * <p>
   * The following example shows how to bootstrap against a node with custom KV and management ports:
   * <pre>
   * Set<SeedNode> seedNodes = new HashSet<>(Collections.singletonList(
   *   SeedNode.create("127.0.0.1", Optional.of(12000), Optional.of(9000))
   * ));
   * Cluster cluster Cluster.connect(seedNodes, clusterOptions("user", "password"));
   * </pre>
   * @param seedNodes the seed nodes used to connect to the cluster.
   * @param options custom options when creating the cluster.
   * @return the instantiated {@link Cluster}.
   */
  public static Cluster connect(final Set<SeedNode> seedNodes, final ClusterOptions options) {
    notNullOrEmpty(seedNodes, "SeedNodes");
    notNull(options, "ClusterOptions");

    final ClusterOptions.Built opts = options.build();
    return new Cluster(extractClusterEnvironment(null, opts), opts.authenticator(), seedNodes);
  }

  /**
   * Creates a new cluster from a {@link ClusterEnvironment}.
   *
   * @param environment the environment to use.
   * @param authenticator the authenticator to use.
   * @param seedNodes the seed nodes to bootstrap from.
   */
  private Cluster(final Supplier<ClusterEnvironment> environment, final Authenticator authenticator,
                  final Set<SeedNode> seedNodes) {
    this.asyncCluster = new AsyncCluster(environment, authenticator, seedNodes);
    this.reactiveCluster = new ReactiveCluster(asyncCluster);
    this.searchIndexManager = new SearchIndexManager(asyncCluster.searchIndexes());
    this.userManager = new UserManager(asyncCluster.users());
    this.bucketManager = new BucketManager(asyncCluster.buckets());
    this.queryIndexManager = new QueryIndexManager(asyncCluster.queryIndexes());
    this.analyticsIndexManager = new AnalyticsIndexManager(this);
    this.eventingFunctionManager = new EventingFunctionManager(asyncCluster.eventingFunctions());
  }

  /**
   * Provides access to the related {@link AsyncCluster}.
   * <p>
   * Note that the {@link AsyncCluster} is considered advanced API and should only be used to get the last drop
   * of performance or if you are building higher-level abstractions on top. If in doubt, we recommend using the
   * {@link #reactive()} API instead.
   */
  public AsyncCluster async() {
    return asyncCluster;
  }

  /**
   * Provides access to the related {@link ReactiveCluster}.
   */
  public ReactiveCluster reactive() {
    return reactiveCluster;
  }

  /**
   * Provides access to the underlying {@link Core}.
   *
   * <p>This is advanced and volatile API - it might change any time without notice. <strong>Use with care!</strong></p>
   */
  @Stability.Volatile
  public Core core() {
    return asyncCluster.core();
  }

  /**
   * The user manager allows to manage users and groups.
   */
  public UserManager users() {
    return userManager;
  }

  /**
   * The bucket manager allows to perform administrative tasks on buckets and their resources.
   */
  public BucketManager buckets() {
    return bucketManager;
  }

  /**
   * The analytics index manager allows to modify and create indexes for the analytics service.
   */
  public AnalyticsIndexManager analyticsIndexes() {
    return analyticsIndexManager;
  }

  /**
   * The query index manager allows to modify and create indexes for the query service.
   */
  public QueryIndexManager queryIndexes() {
    return queryIndexManager;
  }

  /**
   * The search index manager allows to modify and create indexes for the search service.
   */
  public SearchIndexManager searchIndexes() {
    return searchIndexManager;
  }

  /**
   * Provides access to the eventing function management services.
   */
  @Stability.Uncommitted
  public EventingFunctionManager eventingFunctions() {
    return eventingFunctionManager;
  }

  /**
   * Provides access to the used {@link ClusterEnvironment}.
   */
  public ClusterEnvironment environment() {
    return asyncCluster.environment();
  }

  /**
   * Performs a query against the query (N1QL) services.
   *
   * @param statement the N1QL query statement.
   * @return the {@link QueryResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public QueryResult query(final String statement) {
    return query(statement, DEFAULT_QUERY_OPTIONS);
  }

  /**
   * Performs a query against the query (N1QL) services with custom options.
   *
   * @param statement the N1QL query statement as a raw string.
   * @param options the custom options for this query.
   * @return the {@link QueryResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public QueryResult query(final String statement, final QueryOptions options) {
    return block(async().query(statement, options));
  }

  /**
   * Performs an analytics query with default {@link AnalyticsOptions}.
   *
   * @param statement the query statement as a raw string.
   * @return the {@link AnalyticsResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public AnalyticsResult analyticsQuery(final String statement) {
    return analyticsQuery(statement, DEFAULT_ANALYTICS_OPTIONS);
  }

  /**
   * Performs an analytics query with custom {@link AnalyticsOptions}.
   *
   * @param statement the query statement as a raw string.
   * @param options the custom options for this query.
   * @return the {@link AnalyticsResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public AnalyticsResult analyticsQuery(final String statement, final AnalyticsOptions options) {
    return block(async().analyticsQuery(statement, options));
  }

  /**
   * Performs a Full Text Search (FTS) query with default {@link SearchOptions}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @return the {@link SearchRequest} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public SearchResult searchQuery(final String indexName, final SearchQuery query) {
    return searchQuery(indexName, query, DEFAULT_SEARCH_OPTIONS);
  }

  /**
   * Performs a Full Text Search (FTS) query with custom {@link SearchOptions}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @param options the custom options for this query.
   * @return the {@link SearchRequest} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public SearchResult searchQuery(final String indexName, final SearchQuery query, final SearchOptions options) {
    return block(asyncCluster.searchQuery(indexName, query, options));
  }

  /**
   * Opens a {@link Bucket} with the given name.
   *
   * @param bucketName the name of the bucket to open.
   * @return a {@link Bucket} once opened.
   */
  public Bucket bucket(final String bucketName) {
    return bucketCache.computeIfAbsent(bucketName, n -> new Bucket(asyncCluster.bucket(n)));
  }

  /**
   * Performs a non-reversible disconnect of this {@link Cluster}.
   * <p>
   * If this method is used, the default disconnect timeout on the environment is used. Please use the companion
   * overload ({@link #disconnect(Duration)} if you want to provide a custom duration.
   * <p>
   * If a custom {@link ClusterEnvironment} has been passed in during connect, it is <strong>VERY</strong> important to
   * shut it down after calling this method. This will prevent any in-flight tasks to be stopped prematurely.
   */
  public void disconnect() {
    block(asyncCluster.disconnect());
  }

  /**
   * Performs a non-reversible disconnect of this {@link Cluster}.
   * <p>
   * If a custom {@link ClusterEnvironment} has been passed in during connect, it is <strong>VERY</strong> important to
   * shut it down after calling this method. This will prevent any in-flight tasks to be stopped prematurely.
   *
   * @param timeout allows to override the default disconnect duration.
   */
  public void disconnect(final Duration timeout) {
    block(asyncCluster.disconnect(timeout));
  }

  /**
   * Runs a diagnostic report on the current state of the cluster from the SDKs point of view.
   * <p>
   * Please note that it does not perform any I/O to do this, it will only use the current known state of the cluster
   * to assemble the report (so, if for example no N1QL query has been run the socket pool might be empty and as
   * result not show up in the report).
   *
   * @return the {@link DiagnosticsResult} once complete.
   */
  public DiagnosticsResult diagnostics() {
    return block(asyncCluster.diagnostics(DEFAULT_DIAGNOSTICS_OPTIONS));
  }

  /**
   * Runs a diagnostic report with custom options on the current state of the cluster from the SDKs point of view.
   * <p>
   * Please note that it does not perform any I/O to do this, it will only use the current known state of the cluster
   * to assemble the report (so, if for example no N1QL query has been run the socket pool might be empty and as
   * result not show up in the report).
   *
   * @param options options that allow to customize the report.
   * @return the {@link DiagnosticsResult} once complete.
   */
  public DiagnosticsResult diagnostics(final DiagnosticsOptions options) {
    return block(asyncCluster.diagnostics(options));
  }

  /**
   * Performs application-level ping requests against services in the couchbase cluster.
   * <p>
   * Note that this operation performs active I/O against services and endpoints to assess their health. If you do
   * not wish to perform I/O, consider using the {@link #diagnostics()} instead. You can also combine the functionality
   * of both APIs as needed, which is {@link #waitUntilReady(Duration)} is doing in its implementation as well.
   *
   * @return the {@link PingResult} once complete.
   */
  public PingResult ping() {
    return block(asyncCluster.ping());
  }

  /**
   * Performs application-level ping requests with custom options against services in the couchbase cluster.
   * <p>
   * Note that this operation performs active I/O against services and endpoints to assess their health. If you do
   * not wish to perform I/O, consider using the {@link #diagnostics(DiagnosticsOptions)} instead. You can also combine
   * the functionality of both APIs as needed, which is {@link #waitUntilReady(Duration)} is doing in its
   * implementation as well.
   *
   * @return the {@link PingResult} once complete.
   */
  public PingResult ping(final PingOptions options) {
    return block(asyncCluster.ping(options));
  }

  /**
   * Waits until the desired {@link ClusterState} is reached.
   * <p>
   * This method will wait until either the cluster state is "online", or the timeout is reached. Since the SDK is
   * bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
   * and usable before moving on.
   *
   * @param timeout the maximum time to wait until readiness.
   */
  public void waitUntilReady(final Duration timeout) {
    block(asyncCluster.waitUntilReady(timeout));
  }

  /**
   * Waits until the desired {@link ClusterState} is reached.
   * <p>
   * This method will wait until either the cluster state is "online" by default, or the timeout is reached. Since the
   * SDK is bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
   * and usable before moving on. You can tune the properties through {@link WaitUntilReadyOptions}.
   *
   * @param timeout the maximum time to wait until readiness.
   * @param options the options to customize the readiness waiting.
   */
  public void waitUntilReady(final Duration timeout, final WaitUntilReadyOptions options) {
    block(asyncCluster.waitUntilReady(timeout, options));
  }

  /**
   * Allows access to transactions.
   *
   * @return the {@link Transactions} interface.
   */
  @Stability.Uncommitted
  public Transactions transactions() {
    return new Transactions(core(), environment().jsonSerializer());
  }
}

