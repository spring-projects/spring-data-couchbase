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
import com.couchbase.client.java.query.dsl.path.WherePath;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.CouchbasePagingAndSortingRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.CountFragment;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.util.Assert;

/**
 * A {@link CouchbasePagingAndSortingRepository} implementation. It uses N1QL for its {@link PagingAndSortingRepository}
 * method implementation.
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
    Expression whereCriteria = N1qlUtils.createWhereFilterForEntity(null, getCouchbaseOperations().getConverter(),
        getEntityInformation());

    //apply the paging
    Statement pageStatement = selectFrom.where(whereCriteria).limit(pageable.getPageSize()).offset(pageable.getOffset());

    //fire the query
    N1qlQuery query = N1qlQuery.simple(pageStatement, N1qlParams.build().consistency(consistency));
    List<T> pageContent = getCouchbaseOperations().findByN1QL(query, getEntityInformation().getJavaType());

    //return the list as a Page
    return new PageImpl<T>(pageContent, pageable, totalCount);
  }
}
