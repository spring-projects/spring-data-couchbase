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

import java.util.Iterator;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.LimitPath;
import com.couchbase.client.java.query.dsl.path.OrderByPath;
import com.couchbase.client.java.query.dsl.path.WherePath;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.repository.query.support.N1qlQueryCreatorUtils;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * This {@link AbstractQueryCreator Query Creator} is responsible for parsing a {@link PartTree} (representing
 * a method name) into the WHERE clause of a N1QL query.
 * <p>
 * In the following, "field" represents the path in JSON deduced from the part of the method name. "a" and "b" represent
 * the values of next consumed method parameters. "array" represent a {@link JsonArray} constructed from the next method
 * parameter value (if a collection or array, contained values are used to fill the array, otherwise it's a single item
 * array).
 * <br/>
 * Here are the {@link Part.Type} supported (<code>field</code>:
 * <ul>
 *   <li><b>BETWEEN:</b> field BETWEEN a AND b</li>
 *   <li><b>IS_NOT_NULL:</b> field IS NOT NULL</li>
 *   <li><b>IS_NULL:</b> field IS NULL</li>
 *   <li><b>NEGATING_SIMPLE_PROPERTY:</b> - field != a</li>
 *   <li><b>SIMPLE_PROPERTY:</b> - field = a</li>
 *   <li><b>LESS_THAN:</b> field &lt; a</li>
 *   <li><b>LESS_THAN_EQUAL:</b> field &lt;= a</li>
 *   <li><b>GREATER_THAN_EQUAL:</b> field &gt;= a</li>
 *   <li><b>GREATER_THAN:</b> field &gt; a</li>
 *   <li><b>BEFORE:</b> field &lt; a</li>
 *   <li><b>AFTER:</b> field &gt; a</li>
 *   <li><b>NOT_LIKE:</b> field NOT LIKE "a" - a should be a String containing % and _ (matching n and 1 characters)</li>
 *   <li><b>LIKE:</b> field LIKE "a" - a should be a String containing % and _ (matching n and 1 characters)</li>
 *   <li><b>STARTING_WITH:</b> field LIKE "a%" - a should be a String prefix</li>
 *   <li><b>ENDING_WITH:</b> field LIKE "%a" - a should be a String suffix</li>
 *   <li><b>NOT_CONTAINING:</b> field NOT LIKE "%a%" - a should be a String</li>
 *   <li><b>CONTAINING:</b> field LIKE "%a%" - a should be a String</li>
 *   <li><b>NOT_IN:</b> field NOT IN array - note that the next parameter value (or its children if a collection/array)
 *   should be compatible for storage in a {@link JsonArray})</li>
 *   <li><b>IN:</b> field IN array - note that the next parameter value (or its children if a collection/array) should
 *    be compatible for storage in a {@link JsonArray})</li>
 *   <li><b>TRUE:</b> field = TRUE</li>
 *   <li><b>FALSE:</b> field = FALSE</li>
 *   <li><b>REGEX:</b> REGEXP_LIKE(field, "a") - note that the ignoreCase is ignored here, a is a regular expression
 *   in String form</li>
 *   <li><b>EXISTS:</b> field IS NOT MISSING - used to verify that the JSON contains this attribute</li>
 * </ul>
 * <br/>
 * The following are not supported and will throw an {@link IllegalArgumentException} if encountered:
 * <ul>
 *   <li><b>NEAR, WITHIN:</b> geospatial is not supported in N1QL as of now</li>
 * </ul>
 * </p>
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 */
public class N1qlQueryCreator extends AbstractQueryCreator<LimitPath, Expression> {

  private final WherePath selectFrom;
  private final CouchbaseConverter converter;
  private final CouchbaseQueryMethod queryMethod;
  private final ParameterAccessor accessor;

  public N1qlQueryCreator(PartTree tree, ParameterAccessor parameters, WherePath selectFrom,
                          CouchbaseConverter converter, CouchbaseQueryMethod queryMethod) {
    super(tree, parameters);
    this.selectFrom = selectFrom;
    this.converter = converter;
    this.queryMethod = queryMethod;
    this.accessor = parameters;
  }

  @Override
  protected Expression create(Part part, Iterator<Object> iterator) {
    return N1qlQueryCreatorUtils.prepareExpression(converter, part, iterator);
  }

  @Override
  protected Expression and(Part part, Expression base, Iterator<Object> iterator) {
    if (base == null) {
      return create(part, iterator);
    }

    return base.and(create(part, iterator));
  }

  @Override
  protected Expression or(Expression base, Expression criteria) {
    return base.or(criteria);
  }

  @Override
  protected LimitPath complete(Expression criteria, Sort sort) {
    Expression whereCriteria = N1qlUtils.createWhereFilterForEntity(criteria, this.converter, this.queryMethod.getEntityInformation());

    OrderByPath selectFromWhere = selectFrom.where(whereCriteria);

    //sort of the Pageable takes precedence over the sort in the query name
    if ((queryMethod.isPageQuery() || queryMethod.isSliceQuery()) && accessor.getPageable() != null) {
      Pageable pageable = accessor.getPageable();
      if (pageable.getSort() != null) {
        sort = pageable.getSort();
      }
    }

    if (sort != null) {
      com.couchbase.client.java.query.dsl.Sort[] cbSorts = N1qlUtils.createSort(sort, converter);
      return selectFromWhere.orderBy(cbSorts);
    }
    return selectFromWhere;
  }

}
