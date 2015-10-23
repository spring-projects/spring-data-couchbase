/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.support;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Repository base implementation for Couchbase.
 *
 * @author Michael Nitschinger
 */
public class SimpleCouchbaseRepository<T, ID extends Serializable> implements CouchbaseRepository<T, ID> {

  /**
   * Holds the reference to the {@link org.springframework.data.couchbase.core.CouchbaseTemplate}.
   */
  private final CouchbaseOperations couchbaseOperations;

  /**
   * Contains information about the entity being used in this repository.
   */
  private final CouchbaseEntityInformation<T, String> entityInformation;

  /**
   * Custom ViewMetadataProvider.
   */
  private ViewMetadataProvider viewMetadataProvider;

  /**
   * Create a new Repository.
   *
   * @param metadata the Metadata for the entity.
   * @param couchbaseOperations the reference to the template used.
   */
  public SimpleCouchbaseRepository(final CouchbaseEntityInformation<T, String> metadata, final CouchbaseOperations couchbaseOperations) {
    Assert.notNull(couchbaseOperations);
    Assert.notNull(metadata);

    entityInformation = metadata;
    this.couchbaseOperations = couchbaseOperations;
  }

  /**
   * Configures a custom {@link ViewMetadataProvider} to be used to detect {@link View}s to be applied to queries.
   *
   * @param viewMetadataProvider that is used to lookup any annotated View on a query method.
   */
  public void setViewMetadataProvider(final ViewMetadataProvider viewMetadataProvider) {
    this.viewMetadataProvider = viewMetadataProvider;
  }

  @Override
  public <S extends T> S save(S entity) {
    Assert.notNull(entity, "Entity must not be null!");

    couchbaseOperations.save(entity);
    return entity;
  }

  @Override
  public <S extends T> Iterable<S> save(Iterable<S> entities) {
    Assert.notNull(entities, "The given Iterable of entities must not be null!");

    List<S> result = new ArrayList<S>();
    for (S entity : entities) {
      save(entity);
      result.add(entity);
    }
    return result;
  }

  @Override
  public T findOne(ID id) {
    Assert.notNull(id, "The given id must not be null!");
    return couchbaseOperations.findById(id.toString(), entityInformation.getJavaType());
  }

  @Override
  public boolean exists(ID id) {
    Assert.notNull(id, "The given id must not be null!");
    return couchbaseOperations.exists(id.toString());
  }

  @Override
  public void delete(ID id) {
    Assert.notNull(id, "The given id must not be null!");
    couchbaseOperations.remove(id.toString());
  }

  @Override
  public void delete(T entity) {
    Assert.notNull(entity, "The given id must not be null!");
    couchbaseOperations.remove(entity);
  }

  @Override
  public void delete(Iterable<? extends T> entities) {
    Assert.notNull(entities, "The given Iterable of entities must not be null!");
    for (T entity : entities) {
      couchbaseOperations.remove(entity);
    }
  }

  @Override
  public Iterable<T> findAll() {
    final ResolvedView resolvedView = determineView();
    ViewQuery query = ViewQuery.from(resolvedView.getDesignDocument(), resolvedView.getViewName());
    query.reduce(false);
    return couchbaseOperations.findByView(query, entityInformation.getJavaType());
  }

  @Override
  public Iterable<T> findAll(final Iterable<ID> ids) {
    final ResolvedView resolvedView = determineView();
    ViewQuery query = ViewQuery.from(resolvedView.getDesignDocument(), resolvedView.getViewName());
    query.reduce(false);
    JsonArray keys = JsonArray.create();
    for (ID id : ids) {
      keys.add(id);
    }
    query.keys(keys);

    return couchbaseOperations.findByView(query, entityInformation.getJavaType());
  }

  @Override
  public long count() {
    final ResolvedView resolvedView = determineView();
    ViewQuery query = ViewQuery.from(resolvedView.getDesignDocument(), resolvedView.getViewName());
    query.reduce(true);

    ViewResult response = couchbaseOperations.queryView(query);

    long count = 0;
    for (ViewRow row : response) {
      count += Long.parseLong(String.valueOf(row.value()));
    }

    return count;
  }

  @Override
  public void deleteAll() {
    final ResolvedView resolvedView = determineView();
    ViewQuery query = ViewQuery.from(resolvedView.getDesignDocument(), resolvedView.getViewName());
    query.reduce(false);

    ViewResult response = couchbaseOperations.queryView(query);
    for (ViewRow row : response) {
      couchbaseOperations.remove(row.id());
    }
  }

  /**
   * Returns the underlying operation template.
   *
   * @return the underlying template.
   */
  protected CouchbaseOperations getCouchbaseOperations() {
    return couchbaseOperations;
  }

  /**
   * Returns the information for the underlying template.
   *
   * @return the underlying entity information.
   */
  protected CouchbaseEntityInformation<T, String> getEntityInformation() {
    return entityInformation;
  }

  /**
   * Resolve a View based upon:
   * <p/>
   * 1. Any @View annotation that is present
   * 2. If none are found, default designDocument to be the entity name (lowercase) and viewName to be "all".
   *
   * @return ResolvedView containing the designDocument and viewName.
   */
  private ResolvedView determineView() {
    String designDocument = StringUtils.uncapitalize(entityInformation.getJavaType().getSimpleName());
    String viewName = "all";

    final View view = viewMetadataProvider.getView();

    if (view != null) {
      designDocument = view.designDocument();
      viewName = view.viewName();
    }

    return new ResolvedView(designDocument, viewName);
  }

  /**
   * Simple holder to allow an easier exchange of information.
   */
  private final class ResolvedView {

    private final String designDocument;
    private final String viewName;

    public ResolvedView(final String designDocument, final String viewName) {
      this.designDocument = designDocument;
      this.viewName = viewName;
    }

    private String getDesignDocument() {
      return designDocument;
    }

    private String getViewName() {
      return viewName;
    }
  }

}
