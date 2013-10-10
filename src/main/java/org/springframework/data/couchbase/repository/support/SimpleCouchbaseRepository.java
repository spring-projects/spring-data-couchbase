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

import com.couchbase.client.protocol.views.ComplexKey;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.view.View;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

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
   * Contains information about this repository.
   */
  private final RepositoryInformation repositoryInformation;

  /**
   * Convenience to hold all declared methods on the interface since we don't want to iterate every time!
   */
  private final Method[] allDeclaredMethods;

  /**
   * Create a new Repository.
   *
   * @param metadata the Metadata for the entity.
   * @param couchbaseOperations the reference to the template used.
   */
  public SimpleCouchbaseRepository(final CouchbaseEntityInformation<T, String> metadata,
                                   final CouchbaseOperations couchbaseOperations,
                                   final RepositoryInformation repositoryInformation) {
    Assert.notNull(couchbaseOperations);
    Assert.notNull(metadata);
    Assert.notNull(repositoryInformation);

    entityInformation = metadata;
    this.couchbaseOperations = couchbaseOperations;
    this.repositoryInformation = repositoryInformation;
    allDeclaredMethods = ReflectionUtils.getAllDeclaredMethods(repositoryInformation.getRepositoryInterface());
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
    String design = entityInformation.getJavaType().getSimpleName().toLowerCase();
    String viewName = "all";

    final Method findAllMethod = repositoryInformation.getCrudMethods().getFindAllMethod();
    final View view = findAllMethod.getAnnotation(View.class);
    if (view != null) {
      design = view.design();
      viewName = view.view();
    }

    return couchbaseOperations.findByView(design, viewName, new Query().setReduce(false), entityInformation.getJavaType());
  }

  @Override
  public Iterable<T> findAll(final Iterable<ID> ids) {
    String design = entityInformation.getJavaType().getSimpleName().toLowerCase();
    String viewName = "all";

    final Method findAllMethod = repositoryInformation.getCrudMethods().getFindAllMethod();
    final View view = findAllMethod.getAnnotation(View.class);
    if (view != null) {
      design = view.design();
      viewName = view.view();
    }

    Query query = new Query();
    query.setReduce(false);
    query.setKeys(ComplexKey.of(ids));

    return couchbaseOperations.findByView(design, viewName, query, entityInformation.getJavaType());
  }

  @Override
  public long count() {
    String design = entityInformation.getJavaType().getSimpleName().toLowerCase();
    String viewName = "all";

    for (final Method method : allDeclaredMethods) {
      if (method.getName().equals("count")) {
        final View view = method.getAnnotation(View.class);
        if (view != null) {
          design = view.design();
          viewName = view.view();
        }
      }
    }

    Query query = new Query();
    query.setReduce(true);

    ViewResponse response = couchbaseOperations.queryView(design, viewName, query);

    long count = 0;
    for (ViewRow row : response) {
      count += Long.parseLong(row.getValue());
    }

    return count;
  }

  @Override
  public void deleteAll() {
    String design = entityInformation.getJavaType().getSimpleName().toLowerCase();
    String viewName = "all";

    final Method deleteMethod = repositoryInformation.getCrudMethods().getDeleteMethod();
    final View view = deleteMethod.getAnnotation(View.class);
    if (view != null) {
      design = view.design();
      viewName = view.view();
    }

    Query query = new Query();
    query.setReduce(false);

    ViewResponse response = couchbaseOperations.queryView(design, viewName, query);
    for (ViewRow row : response) {
      couchbaseOperations.remove(row.getId());
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

}
