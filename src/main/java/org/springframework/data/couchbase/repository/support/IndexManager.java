/*
 * Copyright 2012-2019 the original author or authors
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

package org.springframework.data.couchbase.repository.support;

import java.util.Collections;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.CreateQueryIndexOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


import org.springframework.context.annotation.Profile;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.repository.core.RepositoryInformation;

/**
 * {@link IndexManager} is responsible for automatic index creation according to the provided metadata and
 * various index annotations (if not null).
 * <p/>
 * Index creation will be attempted in parallel using the asynchronous APIs, but the overall process is still blocking.
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 */
public class IndexManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexManager.class);

  private static final String TEMPLATE_MAP_FUNCTION = "function (doc, meta) { if(doc.%s == \"%s\") { emit(meta.id, null); } }";

  private static final JsonObject SUCCESS_MARKER = JsonObject.empty();

  /** True if this index manager should ignore N1QL PRIMARY creation annotations */
  private boolean ignoreN1qlPrimary;
  /** True if this index manager should ignore N1QL SECONDARY creation annotations */
  private boolean ignoreN1qlSecondary;

  /** To perform index creation, you need a reference to the Cluster */
  private Cluster cluster;
  /**
   * Construct an IndexManager that can be used as a Bean in a {@link Profile @Profile} annotated configuration
   * in order to activate only all or part of automatic index creations in some contexts (like activating it in Dev but
   * not in Prod).
   *
   * @param processN1qlPrimary true to process, false to ignore {@link N1qlPrimaryIndexed} annotations.
   * @param processN1qlSecondary true to process, false to ignore {@link N1qlSecondaryIndexed} annotations.
   */
  public IndexManager(Cluster cluster, boolean processN1qlPrimary, boolean processN1qlSecondary) {
    this.cluster = cluster;
    this.ignoreN1qlPrimary = !processN1qlPrimary;
    this.ignoreN1qlSecondary = !processN1qlSecondary;
  }

  /**
   * Construct a default IndexManager that process all three types of automatic index creations.
   */
  public IndexManager(Cluster cluster) {
    this(cluster, true, true);
  }

  /**
   * @return true if this IndexManager ignores {@link N1qlPrimaryIndexed} annotations.
   */
  public boolean isIgnoreN1qlPrimary() {
    return ignoreN1qlPrimary;
  }

  /**
   * @return true if this IndexManager ignores {@link N1qlSecondaryIndexed} annotations.
   */
  public boolean isIgnoreN1qlSecondary() {
    return ignoreN1qlSecondary;
  }

  /**
   * Build the relevant indexes according to the provided annotation and repository metadata, in parallel but blocking
   * until all relevant indexes are created. Existing indexes will be detected and skipped.
   * <p/>
   * Note that this IndexManager could be configured to ignore some of the annotation types.
   * In case of multiple errors, a {@link CompositeException} can be raised with up to 3 causes (one per type of index).
   *
   * @param metadata the repository's metadata (allowing to find out the type of entity stored, the key under which type
   *  information is stored, etc...).
   * @param n1qlPrimaryIndexed the annotation for creation of a N1QL-based primary index (generic).
   * @param n1qlSecondaryIndexed the annotation for creation of a N1QL-based secondary index (specific to the repository
   *   stored entity).
   * @param couchbaseOperations the template to use for index creation.
   * @throws CompositeException when several errors (for multiple index types) have been raised.
   */
  // TODO: remove ViewIndexed
  public void buildIndexes(RepositoryInformation metadata, N1qlPrimaryIndexed n1qlPrimaryIndexed,
                            N1qlSecondaryIndexed n1qlSecondaryIndexed, CouchbaseOperations couchbaseOperations) {
    Mono<Void> viewAsync = Mono.empty();
    Mono<Void> n1qlPrimaryAsync = Mono.empty();
    Mono<Void> n1qlSecondaryAsync = Mono.empty();

    // We now _must_ have a primary index, so skip this only if specifically asked
    if (!ignoreN1qlPrimary) {
      n1qlPrimaryAsync = buildN1qlPrimary(metadata, couchbaseOperations.getCouchbaseBucket());
    }

    /* TODO: figure out why secondary indexes are failing
      if (n1qlSecondaryIndexed != null && !ignoreN1qlSecondary) {
      n1qlSecondaryAsync = buildN1qlSecondary(n1qlSecondaryIndexed, metadata, couchbaseOperations.getCouchbaseBucket(), couchbaseOperations.getConverter().getTypeKey());
    }*/

    //trigger the builds, wait for the last one, throw CompositeException if errors
    Flux.mergeDelayError(1, viewAsync, n1qlPrimaryAsync, n1qlSecondaryAsync).blockLast();
  }

  /**
   * Build the relevant indexes according to the provided annotation and repository metadata, in parallel but blocking
   * until all relevant indexes are created. Existing indexes will be detected and skipped.
   * <p/>
   * Note that this IndexManager could be configured to ignore some of the annotation types.
   * In case of multiple errors, a {@link CompositeException} can be raised with up to 3 causes (one per type of index).
   *
   * @param metadata the repository's metadata (allowing to find out the type of entity stored, the key under which type
   *  information is stored, etc...).
   * @param n1qlPrimaryIndexed the annotation for creation of a N1QL-based primary index (generic).
   * @param n1qlSecondaryIndexed the annotation for creation of a N1QL-based secondary index (specific to the repository
   *   stored entity).
   * @param rxjava1CouchbaseOperations the template to use for index creation.
   * @throws CompositeException when several errors (for multiple index types) have been raised.
   */
  public void buildIndexes(RepositoryInformation metadata, N1qlPrimaryIndexed n1qlPrimaryIndexed,
                           N1qlSecondaryIndexed n1qlSecondaryIndexed, RxJavaCouchbaseOperations rxjava1CouchbaseOperations) {
    Mono<Void> n1qlPrimaryAsync = Mono.empty();
    Mono<Void> n1qlSecondaryAsync = Mono.empty();

    if (n1qlPrimaryIndexed != null && !ignoreN1qlPrimary) {
      n1qlPrimaryAsync = buildN1qlPrimary(metadata, rxjava1CouchbaseOperations.getCouchbaseBucket());
    }
    /* TODO: figure this out - fails so commenting out just for now

    if (n1qlSecondaryIndexed != null && !ignoreN1qlSecondary) {
      n1qlSecondaryAsync = buildN1qlSecondary(n1qlSecondaryIndexed, metadata, rxjava1CouchbaseOperations.getCouchbaseBucket(), rxjava1CouchbaseOperations.getConverter().getTypeKey());
    }*/

    //trigger the builds, wait for the last one, throw CompositeException if errors

    Flux.mergeDelayError(1, n1qlPrimaryAsync, n1qlSecondaryAsync).blockLast();
  }

  private Mono<Void> buildN1qlPrimary(final RepositoryInformation metadata, Bucket bucket) {
    final String bucketName = bucket.name();
    return Mono.fromFuture(cluster.async().queryIndexes()
            .createPrimaryIndex(bucketName, CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions().ignoreIfExists(true))
    );
  }

  private Mono<Void> buildN1qlSecondary(N1qlSecondaryIndexed config, final RepositoryInformation metadata, Bucket bucket, String typeKey) {
    final String bucketName = bucket.name();
    final String indexName = config.indexName();
    final String type = metadata.getDomainType().getName();

    return Mono.fromFuture(cluster.async().queryIndexes()
            .createIndex(bucketName, indexName, Collections.singletonList(typeKey),
                    CreateQueryIndexOptions.createQueryIndexOptions().with(typeKey, type).ignoreIfExists(true))
    );
  }
}
