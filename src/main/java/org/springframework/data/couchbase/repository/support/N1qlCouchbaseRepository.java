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

import java.io.Serializable;
import java.util.List;

import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.SimpleN1qlQuery;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.GroupByPath;
import com.couchbase.client.java.query.dsl.path.LimitPath;
import com.couchbase.client.java.query.dsl.path.WherePath;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.CouchbasePagingAndSortingRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.CountFragment;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.domain.*;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.util.Assert;

/**
 * A {@link CouchbasePagingAndSortingRepository} implementation. It uses N1QL for its {@link PagingAndSortingRepository}
 * method implementation.
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 */
public class N1qlCouchbaseRepository<T, ID extends Serializable>
    extends SimpleCouchbaseRepository<T, ID>
    implements CouchbasePagingAndSortingRepository<T, ID> {

  /**
   * Create a new Repository.
   *
   * @param metadata the Metadata for the entity.
   * @param couchbaseOperations the reference to the template used.
   */
  public N1qlCouchbaseRepository(CouchbaseEntityInformation<T, String> metadata, CouchbaseOperations couchbaseOperations) {
    super(metadata, couchbaseOperations);
  }

  @Override
  public Iterable<T> findAll(Sort sort) {
    Assert.notNull(sort);

    //prepare elements of the query
    WherePath selectFrom = N1qlUtils.createSelectFromForEntity(getCouchbaseOperations().getCouchbaseBucket().name());
    Expression whereCriteria = N1qlUtils.createWhereFilterForEntity(null, getCouchbaseOperations().getConverter(),
        getEntityInformation());

    //apply the sort
    com.couchbase.client.java.query.dsl.Sort[] orderings = N1qlUtils.createSort(sort, getCouchbaseOperations().getConverter());
    Statement st = selectFrom.where(whereCriteria).orderBy(orderings);

    //fire the query
    ScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
    N1qlQuery query = N1qlQuery.simple(st, N1qlParams.build().consistency(consistency));
    return getCouchbaseOperations().findByN1QL(query, getEntityInformation().getJavaType());
  }

  @Override
  public Page<T> findAll(Pageable pageable) {
    Assert.notNull(pageable);
    ScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();

    //prepare the count total query
    Statement countStatement = N1qlUtils.createCountQueryForEntity(getCouchbaseOperations().getCouchbaseBucket().name(),
        getCouchbaseOperations().getConverter(), getEntityInformation());
    SimpleN1qlQuery countQuery = N1qlQuery.simple(countStatement, N1qlParams.build().consistency(consistency));

        //TODO how to avoid to do that more than once?
        //fire the count query and get total count
        List < CountFragment > countResult = getCouchbaseOperations().findByN1QLProjection(countQuery, CountFragment.class);
    long totalCount = countResult == null || countResult.isEmpty() ? 0 : countResult.get(0).count;

    //prepare elements of the data query
    WherePath selectFrom = N1qlUtils.createSelectFromForEntity(getCouchbaseOperations().getCouchbaseBucket().name());

    //add where criteria
    Expression whereCriteria = N1qlUtils.createWhereFilterForEntity(null, getCouchbaseOperations().getConverter(),
        getEntityInformation());
    GroupByPath groupBy = selectFrom.where(whereCriteria);

    //apply the sort if available
    LimitPath limitPath = groupBy;
    if (pageable.getSort() != null) {
      com.couchbase.client.java.query.dsl.Sort[] orderings = N1qlUtils.createSort(pageable.getSort(),
          getCouchbaseOperations().getConverter());
      limitPath = groupBy.orderBy(orderings);
    }

    //apply the paging
    Statement pageStatement = limitPath.limit(pageable.getPageSize()).offset(pageable.getOffset());

    //fire the query
    N1qlQuery query = N1qlQuery.simple(pageStatement, N1qlParams.build().consistency(consistency));
    List<T> pageContent = getCouchbaseOperations().findByN1QL(query, getEntityInformation().getJavaType());

    //return the list as a Page
    return new PageImpl<T>(pageContent, pageable, totalCount);
  }

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.query.QueryByExampleExecutor#findAll(org.springframework.data.domain.Example)
   */
  @Override
  public <S extends T> List<S> findAll(Example<S> example) {
    Assert.notNull(example);
    WherePath selectFrom = N1qlUtils.createSelectFromForEntity(getCouchbaseOperations().getCouchbaseBucket().name());
    Expression whereCriteria = N1qlUtils.createWhereFilterByExampleForEntity(null, getCouchbaseOperations().getConverter(),
            getEntityInformation(), example);

    Statement st = selectFrom.where(whereCriteria);
    ScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
    N1qlQuery query = N1qlQuery.simple(st, N1qlParams.build().consistency(consistency));
    return getCouchbaseOperations().findByN1QL(query, example.getProbeType());
  }

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.query.QueryByExampleExecutor#findAll(org.springframework.data.domain.Example, org.springframework.data.domain.Sort)
   */
  @Override
  public <S extends T> List<S> findAll(Example<S> example, Sort sort) {
    Assert.notNull(sort);
    Assert.notNull(example);
    WherePath selectFrom = N1qlUtils.createSelectFromForEntity(getCouchbaseOperations().getCouchbaseBucket().name());
    Expression whereCriteria = N1qlUtils.createWhereFilterByExampleForEntity(null, getCouchbaseOperations().getConverter(),
            getEntityInformation(), example);

    com.couchbase.client.java.query.dsl.Sort[] orderings = N1qlUtils.createSort(sort, getCouchbaseOperations().getConverter());
    Statement st = selectFrom.where(whereCriteria).orderBy(orderings);

    ScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
    N1qlQuery query = N1qlQuery.simple(st, N1qlParams.build().consistency(consistency));
    return getCouchbaseOperations().findByN1QL(query, example.getProbeType());
  }

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.query.QueryByExampleExecutor#findOne(org.springframework.data.domain.Example)
   */
  public <S extends T> S findOne(Example<S> example) {
    Assert.notNull(example);
    WherePath selectFrom = N1qlUtils.createSelectFromForEntity(getCouchbaseOperations().getCouchbaseBucket().name());
    Expression whereCriteria = N1qlUtils.createWhereFilterByExampleForEntity(null, getCouchbaseOperations().getConverter(),
            getEntityInformation(), example);

    Statement st = selectFrom.where(whereCriteria).limit(1);
    ScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
    N1qlQuery query = N1qlQuery.simple(st, N1qlParams.build().consistency(consistency));
    List<S> result = getCouchbaseOperations().findByN1QL(query, example.getProbeType());
    return result.size() > 0 ? result.get(0) : null;
  }

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.query.QueryByExampleExecutor#findAll(org.springframework.data.domain.Example, org.springframework.data.domain.Pageable)
   */
  public <S extends T> Page<S> findAll(Example<S> example, Pageable pageable){
    Assert.notNull(pageable);
    Assert.notNull(example);
    long totalCount = this.count(example);

    ScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
    WherePath selectFrom = N1qlUtils.createSelectFromForEntity(getCouchbaseOperations().getCouchbaseBucket().name());
    Expression whereCriteria = N1qlUtils.createWhereFilterByExampleForEntity(null, getCouchbaseOperations().getConverter(),
            getEntityInformation(), example);

    GroupByPath groupBy = selectFrom.where(whereCriteria);
    LimitPath limitPath = groupBy;
    if (pageable.getSort() != null) {
      com.couchbase.client.java.query.dsl.Sort[] orderings = N1qlUtils.createSort(pageable.getSort(),
              getCouchbaseOperations().getConverter());
      limitPath = groupBy.orderBy(orderings);
    }
    Statement pageStatement = limitPath.limit(pageable.getPageSize()).offset(pageable.getOffset());
    N1qlQuery query = N1qlQuery.simple(pageStatement, N1qlParams.build().consistency(consistency));
    List<S> pageContent = getCouchbaseOperations().findByN1QL(query, example.getProbeType());
    return new PageImpl<S>(pageContent, pageable, totalCount);
  }

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.query.QueryByExampleExecutor#count(org.springframework.data.domain.Example)
   */
  public <S extends T> long count(Example<S> example) {
    Assert.notNull(example);
    Statement st = N1qlUtils.createCountQueryByExampleForEntity(getCouchbaseOperations().getCouchbaseBucket().name(),
            getCouchbaseOperations().getConverter(), getEntityInformation(), example);

    ScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
    SimpleN1qlQuery countQuery = N1qlQuery.simple(st, N1qlParams.build().consistency(consistency));
    List <CountFragment> countResult = getCouchbaseOperations().findByN1QLProjection(countQuery, CountFragment.class);
    return (countResult == null || countResult.isEmpty()) ? 0 : countResult.get(0).count;
  }

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.query.QueryByExampleExecutor#exists(org.springframework.data.domain.Example)
   */
  public <S extends T> boolean exists(Example<S> example){
    Assert.notNull(example);
    long count = this.count(example);
    return count > 0;
  }
}
