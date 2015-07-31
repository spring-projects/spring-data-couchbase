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

package org.springframework.data.couchbase.repository.query;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.Expression.path;
import static com.couchbase.client.java.query.dsl.functions.AggregateFunctions.count;
import static com.couchbase.client.java.query.dsl.functions.MetaFunctions.meta;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.FromPath;
import com.couchbase.client.java.query.dsl.path.LimitPath;
import com.couchbase.client.java.query.dsl.path.WherePath;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.PartTree;

public class PartTreeN1qlBasedQuery extends AbstractN1qlBasedQuery {

  private final PartTree partTree;

  public PartTreeN1qlBasedQuery(CouchbaseQueryMethod queryMethod, CouchbaseOperations couchbaseOperations) {
    super(queryMethod, couchbaseOperations);
    this.partTree = new PartTree(queryMethod.getName(), queryMethod.getEntityInformation().getJavaType());
  }

  @Override
  protected JsonArray getPlaceholderValues(ParameterAccessor accessor) {
    return JsonArray.empty();
  }

  @Override
  protected Statement getStatement(ParameterAccessor accessor) {
    Expression bucket = i(getCouchbaseOperations().getCouchbaseBucket().name());
    Expression metaId = path(meta(bucket), "id").as(CouchbaseOperations.SELECT_ID);
    Expression metaCas = path(meta(bucket), "cas").as(CouchbaseOperations.SELECT_CAS);

    FromPath select;
    if (partTree.isCountProjection()) {
      select = select(count("*"));
    } else {
      select = select(metaId, metaCas, path(bucket, "*"));
    }
    WherePath selectFrom = select.from(bucket);

    N1qlQueryCreator queryCreator = new N1qlQueryCreator(partTree, accessor, selectFrom,
        getCouchbaseOperations().getConverter(), getQueryMethod());
    LimitPath selectFromWhereOrderBy = queryCreator.createQuery();

    if (partTree.isLimiting()) {
      return selectFromWhereOrderBy.limit(partTree.getMaxResults());
    } else {
      return selectFromWhereOrderBy;
    }
  }
}
