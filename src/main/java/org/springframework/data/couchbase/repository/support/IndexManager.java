/*
 * Copyright 2012-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.support;

import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;

import java.util.Collections;

import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.Index;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.path.index.IndexType;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.exceptions.CompositeException;
import rx.functions.Action1;
import rx.functions.Func1;

import org.springframework.context.annotation.Profile;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseQueryExecutionException;
import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import org.springframework.data.repository.core.RepositoryInformation;

/**
 * {@link IndexManager} is responsible for automatic index creation according to the provided metadata and
 * various index annotations (if not null).
 * <p/>
 * Index creation will be attempted in parallel using the asynchronous APIs, but the overall process is still blocking.
 *
 * @author Simon Baslé
 */
public class IndexManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexManager.class);

  private static final String TEMPLATE_MAP_FUNCTION = "function (doc, meta) { if(doc.%s == \"%s\") { emit(null, null); } }";

  private static final JsonObject SUCCESS_MARKER = JsonObject.empty();

  /** True if this index manager should ignore view creation annotations */
  private boolean ignoreViews;
  /** True if this index manager should ignore N1QL PRIMARY creation annotations */
  private boolean ignoreN1qlPrimary;
  /** True if this index manager should ignore N1QL SECONDARY creation annotations */
  private boolean ignoreN1qlSecondary;


  /**
   * Construct an IndexManager that can be used as a Bean in a {@link Profile @Profile} annotated configuration
   * in order to ignore all or part of automatic index creations in some contexts (like activating it in Dev but
   * not in Prod).
   *
   * @param ignoreViews true to ignore {@link ViewIndexed} annotations.
   * @param ignoreN1qlPrimary true to ignore {@link N1qlPrimaryIndexed} annotations.
   * @param ignoreN1qlSecondary true to ignore {@link N1qlSecondaryIndexed} annotations.
   */
  public IndexManager(boolean ignoreViews, boolean ignoreN1qlPrimary, boolean ignoreN1qlSecondary) {
    this.ignoreViews = ignoreViews;
    this.ignoreN1qlPrimary = ignoreN1qlPrimary;
    this.ignoreN1qlSecondary = ignoreN1qlSecondary;
  }

  /**
   * Construct a default IndexManager that process all three types of automatic index creations.
   */
  public IndexManager() {
    this(false, false, false);
  }

  /**
   * @return true if this IndexManager ignores {@link ViewIndexed} annotations.
   */
  public boolean isIgnoreViews() {
    return ignoreViews;
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
   * @param viewIndexed the annotation for creation of a View-based index.
   * @param n1qlPrimaryIndexed the annotation for creation of a N1QL-based primary index (generic).
   * @param n1qlSecondaryIndexed the annotation for creation of a N1QL-based secondary index (specific to the repository
   *   stored entity).
   * @param couchbaseOperations the template to use for index creation.
   * @throws CompositeException when several errors (for multiple index types) have been raised.
   */
  public void buildIndexes(RepositoryInformation metadata, ViewIndexed viewIndexed, N1qlPrimaryIndexed n1qlPrimaryIndexed,
                            N1qlSecondaryIndexed n1qlSecondaryIndexed, CouchbaseOperations couchbaseOperations) {
    Observable<Void> viewAsync = Observable.empty();
    Observable<Void> n1qlPrimaryAsync = Observable.empty();
    Observable<Void> n1qlSecondaryAsync = Observable.empty();

    if (viewIndexed != null && !ignoreViews) {
      viewAsync = buildAllView(viewIndexed, metadata, couchbaseOperations);
    }

    if (n1qlPrimaryIndexed != null && !ignoreN1qlPrimary) {
      n1qlPrimaryAsync = buildN1qlPrimary(metadata, couchbaseOperations);
    }

    if (n1qlSecondaryIndexed != null && !ignoreN1qlSecondary) {
      n1qlSecondaryAsync = buildN1qlSecondary(n1qlSecondaryIndexed, metadata, couchbaseOperations);
    }

    //trigger the builds, wait for the last one, throw CompositeException if errors
    Observable.mergeDelayError(viewAsync, n1qlPrimaryAsync, n1qlSecondaryAsync)
        .toBlocking()
        .lastOrDefault(null);
  }

  private Observable<Void> buildN1qlPrimary(final RepositoryInformation metadata, CouchbaseOperations couchbaseOperations) {
    final String bucketName = couchbaseOperations.getCouchbaseBucket().name();
    Statement createPrimary = Index.createPrimaryIndex()
        .on(bucketName)
        .using(IndexType.GSI);

    LOGGER.debug("Creating N1QL primary index for repository {}", metadata.getRepositoryInterface().getSimpleName());
    return couchbaseOperations.getCouchbaseBucket().async().query(createPrimary)
        .flatMap(new Func1<AsyncN1qlQueryResult, Observable<JsonObject>>() {
          @Override
          public Observable<JsonObject> call(AsyncN1qlQueryResult asyncN1qlQueryResult) {
            return asyncN1qlQueryResult.errors();
          }
        })
        .defaultIfEmpty(SUCCESS_MARKER)
        .flatMap(new Func1<JsonObject, Observable<Void>>() {
          @Override
          public Observable<Void> call(JsonObject json) {
            if (json == SUCCESS_MARKER) {
              LOGGER.debug("N1QL primary index created for repository {}", metadata.getRepositoryInterface().getSimpleName());
              return Observable.empty();
            } else if (json.getString("msg").contains("Index #primary already exist")) {
              LOGGER.debug("Primary index already exist, skipping");
              return Observable.empty(); //ignore, the index already exist
            } else {
              return Observable.error(new CouchbaseQueryExecutionException(
                  "Cannot create N1QL primary index on " + bucketName + ": " + json));
            }
          }
        });
  }

  private Observable<Void> buildN1qlSecondary(N1qlSecondaryIndexed config, final RepositoryInformation metadata, CouchbaseOperations couchbaseOperations) {
    final String bucketName = couchbaseOperations.getCouchbaseBucket().name();
    final String indexName = config.indexName();
    String typeKey = couchbaseOperations.getConverter().getTypeKey();
    final String type = metadata.getDomainType().getName();

    Statement createIndex = Index.createIndex(indexName)
        .on(bucketName, x(typeKey))
        .where(x(typeKey).eq(s(type)))
        .using(IndexType.GSI);

    LOGGER.debug("Creating N1QL secondary index for repository {}", metadata.getRepositoryInterface().getSimpleName());
    return couchbaseOperations.getCouchbaseBucket().async().query(createIndex)
        .flatMap(new Func1<AsyncN1qlQueryResult, Observable<JsonObject>>() {
          @Override
          public Observable<JsonObject> call(AsyncN1qlQueryResult asyncN1qlQueryResult) {
            return asyncN1qlQueryResult.errors();
          }
        })
        .defaultIfEmpty(SUCCESS_MARKER)
        .flatMap(new Func1<JsonObject, Observable<Void>>() {
          @Override
          public Observable<Void> call(JsonObject json) {
            if (json == SUCCESS_MARKER) {
              LOGGER.debug("N1QL secondary index created for repository {}", metadata.getRepositoryInterface().getSimpleName());
              return Observable.empty();
            } else if (json.getString("msg").contains("Index " + indexName + " already exist")) {
              LOGGER.debug("Secondary index already exist, skipping");
              return Observable.empty(); //ignore, the index already exist
            } else {
              return Observable.error(new CouchbaseQueryExecutionException(
                  "Cannot create N1QL secondary index " + bucketName + "." + indexName + " for " + type + ": " + json));
            }
          }
        });
  }

  private Observable<Void> buildAllView(ViewIndexed config, final RepositoryInformation metadata, CouchbaseOperations couchbaseOperations) {
    if (config == null) return Observable.empty();
    LOGGER.debug("Creating View index index for repository {}", metadata.getRepositoryInterface().getSimpleName());

    BucketManager manager = couchbaseOperations.getCouchbaseBucket().bucketManager();
    String viewName = config.viewName();
    String mapFunction = config.mapFunction();
    if (mapFunction.isEmpty()) {
      String typeKey = couchbaseOperations.getConverter().getTypeKey();
      String type = metadata.getDomainType().getName();

      mapFunction = String.format(TEMPLATE_MAP_FUNCTION, typeKey, type);
    }
    String reduceFunction = config.reduceFunction();
    if ("".equals(reduceFunction)) {
      reduceFunction = null;
    }

    com.couchbase.client.java.view.View view = DefaultView.create(viewName, mapFunction, reduceFunction);
    DesignDocument doc = manager.getDesignDocument(config.designDoc());
    if (doc != null) {
      for (com.couchbase.client.java.view.View existingView : doc.views()) {
        if (existingView.name().equals(viewName)) {
          LOGGER.debug("View index {}/{} already exist, skipping", config.designDoc(), viewName);
          return Observable.empty(); //abort, the view already exist
        }
      }
      doc.views().add(view);
    } else {
      doc = DesignDocument.create(config.designDoc(), Collections.singletonList(view));
    }
    return manager.async().upsertDesignDocument(doc)
        .map(new Func1<DesignDocument, Void>() {
          @Override
          public Void call(DesignDocument designDocument) {
            return null;
          }
        })
        .doOnNext(new Action1<Void>() {
          @Override
          public void call(Void aVoid) {
            LOGGER.debug("View index created for repository {}", metadata.getRepositoryInterface().getSimpleName());
          }
        });
  }
}
