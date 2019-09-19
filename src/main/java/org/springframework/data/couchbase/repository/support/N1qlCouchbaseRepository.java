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

import java.io.Serializable;
import java.util.List;

import com.couchbase.client.java.query.QueryOptions;

import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.N1QLQuery;
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
 *
 * @author Mark Paluch
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
    Assert.notNull(sort, "Sort must not be null!");

    //prepare elements of the query
    N1QLExpression selectFrom = N1qlUtils.createSelectFromForEntity(getCouchbaseOperations().getCouchbaseBucket().name());
    N1QLExpression whereCriteria = N1qlUtils.createWhereFilterForEntity(null, getCouchbaseOperations().getConverter(),
        getEntityInformation());

    //apply the sort
    N1QLExpression[] orderings = N1qlUtils.createSort(sort);
    N1QLExpression st = selectFrom.where(whereCriteria).orderBy(orderings);

    //fire the query
    QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
    N1QLQuery query = new N1QLQuery(st, QueryOptions.queryOptions().scanConsistency(consistency));
    return getCouchbaseOperations().findByN1QL(query, getEntityInformation().getJavaType());
  }

  @Override
  public Page<T> findAll(Pageable pageable) {
    Assert.notNull(pageable, "Pageable must not be null");
    QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();

    //prepare the count total query
    N1QLExpression countStatement = N1qlUtils.createCountQueryForEntity(getCouchbaseOperations().getCouchbaseBucket().name(),
        getCouchbaseOperations().getConverter(), getEntityInformation());
    N1QLQuery countQuery = new N1QLQuery(countStatement, QueryOptions.queryOptions().scanConsistency(consistency));

    // TODO how to avoid to do that more than once?
    //fire the count query and get total count
    List < CountFragment > countResult = getCouchbaseOperations().findByN1QLProjection(countQuery, CountFragment.class);
    long totalCount = countResult == null || countResult.isEmpty() ? 0 : countResult.get(0).count;

    //prepare elements of the data query
    N1QLExpression selectFrom = N1qlUtils.createSelectFromForEntity(getCouchbaseOperations().getCouchbaseBucket().name());

    //add where criteria
    N1QLExpression whereCriteria = N1qlUtils.createWhereFilterForEntity(null, getCouchbaseOperations().getConverter(),
        getEntityInformation());
    N1QLExpression groupBy = selectFrom.where(whereCriteria);

    //apply the sort if available
    N1QLExpression limitPath = groupBy;
    if (pageable.getSort().isSorted()) {
      N1QLExpression[] orderings = N1qlUtils.createSort(pageable.getSort());
      limitPath = groupBy.orderBy(orderings);
    }

    //apply the paging
    N1QLExpression pageStatement = limitPath.limit(pageable.getPageSize()).offset(Math.toIntExact(pageable.getOffset()));

    //fire the query
    N1QLQuery query = new N1QLQuery(pageStatement, QueryOptions.queryOptions().scanConsistency(consistency));
    List<T> pageContent = getCouchbaseOperations().findByN1QL(query, getEntityInformation().getJavaType());

    //return the list as a Page
    return new PageImpl<>(pageContent, pageable, totalCount);
  }
}
