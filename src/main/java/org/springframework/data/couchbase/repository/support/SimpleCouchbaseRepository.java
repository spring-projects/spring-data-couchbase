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

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Repository base implementation for Couchbase.
 *
 * @author Michael Nitschinger
 */
public class SimpleCouchbaseRepository<T, ID extends Serializable> implements CouchbaseRepository<T, ID> {

  private final CouchbaseOperations couchbaseOperations;
  private final CouchbaseEntityInformation<T, String> entityInformation;


  public SimpleCouchbaseRepository(CouchbaseEntityInformation<T, String> metadata, CouchbaseOperations couchbaseOperations) {
    Assert.notNull(couchbaseOperations);
    Assert.notNull(metadata);

    entityInformation = metadata;
    this.couchbaseOperations = couchbaseOperations;
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
    for(S entity : entities) {
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
    for (T entity: entities) {
      couchbaseOperations.remove(entity);
    }
  }

  @Override
  public Iterable<T> findAll() {
    throw new UnsupportedOperationException("findAll is not supported in the current version!");
  }

  @Override
  public Iterable<T> findAll(Iterable<ID> ids) {
    throw new UnsupportedOperationException("findAll is not supported in the current version!");
  }

  @Override
  public long count() {
    throw new UnsupportedOperationException("count is not supported in the current version!");
  }

  @Override
  public void deleteAll() {
    throw new UnsupportedOperationException("deleteAll is not supported in the current version!");
  }

  protected CouchbaseOperations getCouchbaseOperations() {
    return couchbaseOperations;
  }

  protected CouchbaseEntityInformation<T, String> getEntityInformation() {
    return entityInformation;
  }

}
