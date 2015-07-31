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

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.Statement;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;

/**
 * A {@link RepositoryQuery} for Couchbase, based on N1QL and a String statement.
 * The statement can contain placeholders for the {@link #PLACEHOLDER_BUCKET bucket namespace},
 * the {@link #PLACEHOLDER_ENTITY ID and CAS fields} necessary for entity reconstruction or
 * a shortcut that covers {@link #PLACEHOLDER_SELECT_FROM SELECT AND FROM clauses}.
 *
 * @author Simon Baslé
 */
public class StringN1qlBasedQuery extends AbstractN1qlBasedQuery {

  /**
   * Use this placeholder in a {@link org.springframework.data.couchbase.core.view.Query @Query} annotation's inline
   * statement. This will be replaced by the correct <code>SELECT x FROM y</code> clause needed for entity mapping.
   */
  public static final String PLACEHOLDER_SELECT_FROM = "$SELECT_ENTITY$";

  /**
   * Use this placeholder in a {@link org.springframework.data.couchbase.core.view.Query @Query} annotation's inline
   * statement. This will be replaced by the bucket name corresponding to the repository's entity.
   */
  public static final String PLACEHOLDER_BUCKET = "$BUCKET$";

  /**
   * Use this placeholder in a {@link org.springframework.data.couchbase.core.view.Query @Query} annotation's inline
   * statement. This will be replaced by the fields allowing to construct the repository's entity (SELECT clause).
   */
  public static final String PLACEHOLDER_ENTITY = "$ENTITY$";

  /**
   * Use this placeholder in a {@link org.springframework.data.couchbase.core.view.Query @Query} annotation's inline
   * statement WHERE clause. This will be replaced by the expression allowing to only select documents matching the
   * entity's class.
   */
  public static final String PLACEHOLDER_FILTER_TYPE = "$FILTER_TYPE$";

  private final Statement statement;

  public StringN1qlBasedQuery(String statement, CouchbaseQueryMethod queryMethod, CouchbaseOperations couchbaseOperations) {
    super(queryMethod, couchbaseOperations);
    String typeField = getCouchbaseOperations().getConverter().getTypeKey();
    Class<?> typeValue = getQueryMethod().getEntityInformation().getJavaType();
    this.statement = prepare(statement, couchbaseOperations.getCouchbaseBucket().name(), typeField, typeValue);
  }

  protected static Statement prepare(String statement, String bucketName, String typeField, Class<?> typeValue) {
    String b = "`" + bucketName + "`";
    String entity = "META(" + b + ").id AS " + CouchbaseOperations.SELECT_ID +
        ", META(" + b + ").cas AS " + CouchbaseOperations.SELECT_CAS;
    String selectEntity = "SELECT " + entity + ", " + b + ".* FROM " + b;
    String typeSelection = "`" + typeField + "` = \"" + typeValue.getName() + "\"";

    String result = statement;
    if (statement.contains(PLACEHOLDER_SELECT_FROM)) {
      result = result.replaceFirst("\\$SELECT_ENTITY\\$", selectEntity);
    } else {
      if (statement.contains(PLACEHOLDER_BUCKET)) {
        result = result.replaceAll("\\$BUCKET\\$", b);
      }
      if (statement.contains(PLACEHOLDER_ENTITY)) {
        result = result.replaceFirst("\\$ENTITY\\$", entity);
      }
    }

    if (statement.contains(PLACEHOLDER_FILTER_TYPE)) {
      result = result.replaceFirst("\\$FILTER_TYPE\\$", typeSelection);
    }

    return Query.simple(result).statement();
  }

  @Override
  protected JsonArray getPlaceholderValues(ParameterAccessor accessor) {
    JsonArray values = JsonArray.create();
    for (Object value : accessor) {
      values.add(value);
    }
    return values;
  }

  @Override
  public Statement getStatement(ParameterAccessor accessor) {
    return this.statement;
  }

}
