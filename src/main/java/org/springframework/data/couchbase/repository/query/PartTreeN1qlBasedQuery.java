/*
 * Copyright 2012-2017 the original author or authors
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

package org.springframework.data.couchbase.repository.query;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.functions.AggregateFunctions.count;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonValue;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.FromPath;
import com.couchbase.client.java.query.dsl.path.LimitPath;
import com.couchbase.client.java.query.dsl.path.WherePath;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

/**
 * A {@link RepositoryQuery} for Couchbase, based on query derivation
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 */
public class PartTreeN1qlBasedQuery extends AbstractN1qlBasedQuery {

  private final PartTree partTree;

  public PartTreeN1qlBasedQuery(CouchbaseQueryMethod queryMethod, CouchbaseOperations couchbaseOperations) {
    super(queryMethod, couchbaseOperations);
    this.partTree = new PartTree(queryMethod.getName(), queryMethod.getEntityInformation().getJavaType());
  }

  @Override
  protected JsonValue getPlaceholderValues(ParameterAccessor accessor) {
    return JsonArray.empty();
  }

  @Override
  protected Statement getCount(ParameterAccessor accessor, Object[] runtimeParameters) {
    Expression bucket = i(getCouchbaseOperations().getCouchbaseBucket().name());
    WherePath countFrom = select(count("*").as(CountFragment.COUNT_ALIAS)).from(bucket);

    N1qlQueryCreator queryCreator = new N1qlCountQueryCreator(partTree, accessor, countFrom,
        getCouchbaseOperations().getConverter(), getQueryMethod());
    return queryCreator.createQuery();
  }
  @Override
  protected Statement getStatement(ParameterAccessor accessor, Object[] runtimeParameters, ReturnedType returnedType) {
    String bucketName = getCouchbaseOperations().getCouchbaseBucket().name();
    Expression bucket = N1qlUtils.escapedBucket(bucketName);

    FromPath select;
    if (partTree.isCountProjection()) {
      select = select(count("*"));
    } else {
        select = N1qlUtils.createSelectClauseForEntity(bucketName, returnedType, this.getCouchbaseOperations().getConverter());
    }
    WherePath selectFrom = select.from(bucket);

    N1qlQueryCreator queryCreator = new N1qlQueryCreator(partTree, accessor, selectFrom,
        getCouchbaseOperations().getConverter(), getQueryMethod());
    LimitPath selectFromWhereOrderBy = queryCreator.createQuery();

    if (queryMethod.isPageQuery()) {
      Pageable pageable = accessor.getPageable();
      Assert.notNull(pageable, "Pageable must not be null!");
      return selectFromWhereOrderBy.limit(pageable.getPageSize()).offset(Math.toIntExact(pageable.getOffset()));
    } else if (queryMethod.isSliceQuery() && accessor.getPageable() != Pageable.NONE) {
      Pageable pageable = accessor.getPageable();
      Assert.notNull(pageable, "Pageable must not be null!");
      return selectFromWhereOrderBy.limit(pageable.getPageSize() + 1).offset(Math.toIntExact(pageable.getOffset()));
    } else if (partTree.isLimiting()) {
      return selectFromWhereOrderBy.limit(partTree.getMaxResults());
    } else {
      return selectFromWhereOrderBy;
    }
  }

  @Override
  protected boolean useGeneratedCountQuery() {
    return false; //generated count query is just for Page/Slice, not projections
  }
}
