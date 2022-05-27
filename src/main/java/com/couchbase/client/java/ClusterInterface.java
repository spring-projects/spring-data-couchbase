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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.diagnostics.DiagnosticsResult;
import com.couchbase.client.core.diagnostics.PingResult;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.java.analytics.AnalyticsOptions;
//import com.couchbase.client.java.analytics.AnalyticsResult;
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
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;

import java.time.Duration;
import java.util.Set;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.AsyncCluster.extractClusterEnvironment;
import static com.couchbase.client.java.AsyncCluster.seedNodesFromConnectionString;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;

public interface ClusterInterface {

  AsyncCluster async();

  ReactiveCluster reactive();

  @Stability.Volatile
  Core core();

  UserManager users();

  BucketManager buckets();

  AnalyticsIndexManager analyticsIndexes();

  QueryIndexManager queryIndexes();

  SearchIndexManager searchIndexes();

  @Stability.Uncommitted
  EventingFunctionManager eventingFunctions();

  ClusterEnvironment environment();

  QueryResult query(String statement);

  QueryResult query(String statement, QueryOptions options);

  //AnalyticsResult analyticsQuery(String statement);

  // AnalyticsResult analyticsQuery(String statement, AnalyticsOptions options);

  SearchResult searchQuery(String indexName, SearchQuery query);

  SearchResult searchQuery(String indexName, SearchQuery query, SearchOptions options);

  Bucket bucket(String bucketName);

  void disconnect();

  void disconnect(Duration timeout);

  DiagnosticsResult diagnostics();

  DiagnosticsResult diagnostics(DiagnosticsOptions options);

  PingResult ping();

  PingResult ping(PingOptions options);

  void waitUntilReady(Duration timeout);

  void waitUntilReady(Duration timeout, WaitUntilReadyOptions options);

  Transactions transactions();
}
