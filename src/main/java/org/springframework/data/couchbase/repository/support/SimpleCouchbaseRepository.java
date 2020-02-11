/*
 * Copyright 2013-2019 the original author or authors.
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

package org.springframework.data.couchbase.repository.support;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.util.Assert;

import static org.springframework.data.couchbase.core.query.N1QLExpression.x;
import static org.springframework.data.couchbase.core.query.N1QLExpression.s;
import static org.springframework.data.couchbase.core.query.N1QLExpression.i;
import static org.springframework.data.couchbase.core.query.N1QLExpression.select;

/**
 * Repository base implementation for Couchbase.
 *
 * @author Michael Nitschinger
 * @author Mark Paluch
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
    Assert.notNull(metadata, "CouchbaseEntityInformation must not be null!");
    Assert.notNull(couchbaseOperations, "CouchbaseOperations must not be null!");

    entityInformation = metadata;

    // the base query gets all the items by their
    this.couchbaseOperations = couchbaseOperations;
  }

  @Override
  public <S extends T> S save(S entity) {
    throw new UnsupportedOperationException("TODO");

/*    Assert.notNull(entity, "Entity must not be null!");
    couchbaseOperations.upsert(entity);
    return entity;*/
  }

  @Override
  public <S extends T> Iterable<S> saveAll(Iterable<S> entities) {
    Assert.notNull(entities, "The given Iterable of entities must not be null!");

    List<S> result = new ArrayList<S>();
    for (S entity : entities) {
      save(entity);
      result.add(entity);
    }
    return result;
  }

  @Override
  public Optional<T> findById(ID id) {
    throw new UnsupportedOperationException("TODO");

/*    Assert.notNull(id, "The given id must not be null!");
    return Optional.ofNullable(couchbaseOperations.findById(couchbaseOperations.getConverter().convertForWriteIfNeeded(id).toString(), entityInformation.getJavaType()));*/
  }

  @Override
  public boolean existsById(ID id) {
    throw new UnsupportedOperationException("TODO");

/*    Assert.notNull(id, "The given id must not be null!");
    return couchbaseOperations.exists(couchbaseOperations.getConverter().convertForWriteIfNeeded(id).toString());*/
  }

  @Override
  public void deleteById(ID id) {
    throw new UnsupportedOperationException("TODO");
/*
    Assert.notNull(id, "The given id must not be null!");
    couchbaseOperations.remove(couchbaseOperations.getConverter().convertForWriteIfNeeded(id).toString());*/
  }

  @Override
  public void delete(T entity) {
    throw new UnsupportedOperationException("TODO");

/*    Assert.notNull(entity, "The given id must not be null!");
    couchbaseOperations.remove(entity);*/
  }

  @Override
  public void deleteAll(Iterable<? extends T> entities) {
    throw new UnsupportedOperationException("TODO");

/*    Assert.notNull(entities, "The given Iterable of entities must not be null!");
    for (T entity : entities) {
      couchbaseOperations.remove(entity);
    }*/
  }

  @Override
    public Iterable<T> findAll() {
    throw new UnsupportedOperationException("TODO");

/*    N1QLExpression expression = N1qlUtils.createSelectFromForEntity(couchbaseOperations.getBucketName());
      QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
      expression = addClassWhereClause(expression);
      N1QLQuery query = new N1QLQuery(expression, QueryOptions.queryOptions().scanConsistency(consistency));

    return couchbaseOperations.findByN1QL(query, entityInformation.getJavaType());*/
  }

  @Override
  public Iterable<T> findAllById(final Iterable<ID> ids) {
    throw new UnsupportedOperationException("TODO");

/*    N1QLExpression expression = N1qlUtils.createSelectFromForEntity(
            couchbaseOperations.getBucketName())
            .keys(ids);
    expression = addClassWhereClause(expression);
    QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
    N1QLQuery query = new N1QLQuery(expression, QueryOptions.queryOptions().scanConsistency(consistency));

    return couchbaseOperations.findByN1QL(query, entityInformation.getJavaType());*/
  }

  @Override
  public long count() {
    throw new UnsupportedOperationException("TODO");

/*    N1QLExpression expression = select(x("COUNT(*)")).from(i(couchbaseOperations.getBucketName()));
    expression = addClassWhereClause(expression);
    QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
    N1QLQuery query = new N1QLQuery(expression, QueryOptions.queryOptions().scanConsistency(consistency));
    QueryResult res = couchbaseOperations.queryN1QL(query);
    List<JsonObject> obj = res.rowsAsObject();
    return couchbaseOperations.queryN1QL(query).rowsAsObject().get(0).getLong("$1");*/
  }

  @Override
  public void deleteAll() {
    throw new UnsupportedOperationException("TODO");

/*    N1QLExpression expression = x("DELETE").from(i(couchbaseOperations.getBucketName()));
    expression = addClassWhereClause(expression);
    QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
    N1QLQuery query = new N1QLQuery(expression, QueryOptions.queryOptions().scanConsistency(consistency));
    couchbaseOperations.queryN1QL(query);*/
  }

  @Override
  public CouchbaseOperations getCouchbaseOperations() {
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

  private final N1QLExpression addClassWhereClause(N1QLExpression exp) {
    String classString = entityInformation.getJavaType().getCanonicalName();
    return exp.where(x("_class").eq(s(classString)));
  }
}
