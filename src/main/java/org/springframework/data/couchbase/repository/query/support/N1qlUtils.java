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

package org.springframework.data.couchbase.repository.query.support;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.Expression.path;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static com.couchbase.client.java.query.dsl.functions.AggregateFunctions.count;
import static com.couchbase.client.java.query.dsl.functions.MetaFunctions.meta;
import static com.couchbase.client.java.query.dsl.functions.StringFunctions.lower;
import static org.springframework.data.couchbase.core.support.TemplateUtils.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.json.JsonValue;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.functions.TypeFunctions;
import com.couchbase.client.java.query.dsl.path.FromPath;
import com.couchbase.client.java.query.dsl.path.WherePath;
import com.couchbase.client.java.repository.annotation.Field;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.CountFragment;
import org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.repository.core.EntityMetadata;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Utility class to deal with constructing well formed N1QL queries around Spring Data entities, so that
 * the framework can use N1QL to find such entities (eg. restrict the bucket search to a particular type).
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 */
public class N1qlUtils {

  /**
   * A converter that can be used to extract the {@link CouchbasePersistentProperty#getFieldName() fieldName},
   * eg. when one wants a path from {@link PersistentPropertyPath#toDotPath(Converter)} made of escaped field names.
   */
  public static final Converter<? super CouchbasePersistentProperty,String> FIELD_NAME_ESCAPED =
      new Converter<CouchbasePersistentProperty, String>() {
        @Override
        public String convert(CouchbasePersistentProperty source) {
          return "`" + source.getFieldName() + "`";
        }
      };

  /**
   * Escape the given bucketName and produce an {@link Expression}.
   */
  public static Expression escapedBucket(String bucketName) {
    return i(bucketName);
  }

  /**
   * Produce a {@link Statement} that corresponds to the SELECT clause for looking for Spring Data entities
   * stored in Couchbase. Notably it will select the content of the document AND its id and cas and use custom
   * construction of query if required.
   *
   * @param bucketName the bucket that stores the entity documents (will be escaped).
   * @param returnedType Returned type projection information from result processor.
   * @param converter couchbase converter
   * @return the needed SELECT clause of the statement.
   */
  public static FromPath createSelectClauseForEntity(String bucketName, ReturnedType returnedType, CouchbaseConverter converter) {
    Expression bucket = escapedBucket(bucketName);
    Expression metaId = path(meta(bucket), "id").as(SELECT_ID);
    Expression metaCas = path(meta(bucket), "cas").as(SELECT_CAS);
    List<Expression> expList = new ArrayList<Expression>();
    expList.add(metaId);
    expList.add(metaCas);

    if (returnedType != null && returnedType.needsCustomConstruction()) {
      List<String> properties = returnedType.getInputProperties();
      CouchbasePersistentEntity<?> entity = converter.getMappingContext().getPersistentEntity(returnedType.getDomainType());

      for (String property : properties) {
        expList.add(path(bucket, i(entity.getPersistentProperty(property).getFieldName())));
      }
    } else {
      expList.add(path(bucket, "*"));
    }

    Expression[] propertiesExp = new Expression[expList.size()];
    propertiesExp = expList.toArray(propertiesExp);

    return select(propertiesExp);
  }

  /**
   * Produce a {@link Statement} that corresponds to the SELECT clause for looking for Spring Data entities
   * stored in Couchbase. Notably it will select the content of the document AND its id and cas.
   *
   * @param bucketName the bucket that stores the entity documents (will be escaped).
   * @return the needed SELECT clause of the statement.
   */
  public static FromPath createSelectClauseForEntity(String bucketName) {
    return createSelectClauseForEntity(bucketName, null, null);
  }

  /**
   * Produce a {@link Statement} that corresponds to the SELECT...FROM clauses for looking for Spring Data entities
   * stored in Couchbase. Notably it will select the content of the document AND its id and cas FROM the given bucket.
   *
   * @param bucketName the bucket that stores the entity documents (will be escaped).
   * @return the needed SELECT...FROM clauses of the statement.
   */
  public static WherePath createSelectFromForEntity(String bucketName) {
    return createSelectClauseForEntity(bucketName).from(escapedBucket(bucketName));
  }

  /**
   * Produces an {@link Expression} that can serve as a WHERE clause criteria to only select documents in a bucket
   * that matches a particular Spring Data entity (as given by the {@link EntityMetadata} parameter).
   *
   * @param baseWhereCriteria the other criteria of the WHERE clause, or null if none.
   * @param converter the {@link CouchbaseConverter} giving the attribute storing the type information can be extracted.
   * @param entityInformation the expected type information.
   * @return an {@link Expression} to be used as a WHERE clause, that additionally restricts on the given type.
   */
  public static Expression createWhereFilterForEntity(Expression baseWhereCriteria, CouchbaseConverter converter,
                                                      EntityMetadata<?> entityInformation) {
    //add part that filters on type key
    String typeKey = converter.getTypeKey();
    String typeValue = entityInformation.getJavaType().getName();
    Expression typeSelector = i(typeKey).eq(s(typeValue));
    if (baseWhereCriteria == null) {
      baseWhereCriteria = typeSelector;
    } else {
      baseWhereCriteria = x("(" + baseWhereCriteria.toString() + ")").and(typeSelector);
    }
    return baseWhereCriteria;
  }

  /**
   * Given a common {@link PropertyPath}, returns the corresponding {@link PersistentPropertyPath}
   * of {@link CouchbasePersistentProperty} which will allow to discover alternative naming for fields.
   */
  public static PersistentPropertyPath<CouchbasePersistentProperty> getPathWithAlternativeFieldNames(
      CouchbaseConverter converter, PropertyPath property) {
    PersistentPropertyPath<CouchbasePersistentProperty> path = converter.getMappingContext()
        .getPersistentPropertyPath(property);
    return path;
  }

  /**
   * Given a {@link PersistentPropertyPath} of {@link CouchbasePersistentProperty}
   * (see {@link #getPathWithAlternativeFieldNames(CouchbaseConverter, PropertyPath)}),
   * obtain a String representation of the path, separated with dots and using alternative field names.
   */
  public static String getDottedPathWithAlternativeFieldNames(PersistentPropertyPath<CouchbasePersistentProperty> path) {
    return path.toDotPath(FIELD_NAME_ESCAPED);
  }

  /**
   * Create a N1QL {@link com.couchbase.client.java.query.dsl.Sort} out of a Spring Data {@link Sort}. Note that the later
   * must use alternative field names as declared by the {@link Field} annotation on the entity, if any.
   */
  public static com.couchbase.client.java.query.dsl.Sort[] createSort(Sort sort, CouchbaseConverter converter) {
    List<com.couchbase.client.java.query.dsl.Sort> cbSortList = new ArrayList<com.couchbase.client.java.query.dsl.Sort>();
    for (Sort.Order order : sort) {
      String orderProperty = order.getProperty();
      //FIXME the order property should be converted to its corresponding fieldName
      Expression orderFieldName = i(orderProperty);
      if (order.isIgnoreCase()) {
        orderFieldName = lower(TypeFunctions.toString(orderFieldName));
      }
      if (order.isAscending()) {
        cbSortList.add(com.couchbase.client.java.query.dsl.Sort.asc(orderFieldName));
      } else {
        cbSortList.add(com.couchbase.client.java.query.dsl.Sort.desc(orderFieldName));
      }
    }
    return cbSortList.toArray(new com.couchbase.client.java.query.dsl.Sort[cbSortList.size()]);
  }

  /**
   * Creates a full N1QL query that counts total number of the given entity in the bucket.
   *
   * @param bucketName the name of the bucket where data is stored (will be escaped).
   * @param converter the {@link CouchbaseConverter} giving the attribute storing the type information can be extracted.
   * @param entityInformation the counted entity type.
   * @return the N1QL query that counts number of documents matching this entity type.
   */
  public static <T> Statement createCountQueryForEntity(String bucketName, CouchbaseConverter converter, CouchbaseEntityInformation<T, String> entityInformation) {
    return select(count("*").as(CountFragment.COUNT_ALIAS)).from(escapedBucket(bucketName)).where(createWhereFilterForEntity(null, converter, entityInformation));
  }

  /**
   * Creates N1QLQuery object from the statement, query placeholder values and scan consistency
   *
   * @param statement
   * @param queryPlaceholderValues
   * @param scanConsistency
   * @return
   */
  public static N1qlQuery buildQuery(Statement statement, JsonValue queryPlaceholderValues, ScanConsistency scanConsistency) {
    N1qlParams n1qlParams = N1qlParams.build().consistency(scanConsistency);
    N1qlQuery query;

    if (queryPlaceholderValues instanceof JsonObject && !((JsonObject) queryPlaceholderValues).isEmpty()) {
      query = N1qlQuery.parameterized(statement, (JsonObject) queryPlaceholderValues, n1qlParams);
    } else if (queryPlaceholderValues instanceof JsonArray && !((JsonArray) queryPlaceholderValues).isEmpty()) {
      query = N1qlQuery.parameterized(statement, (JsonArray) queryPlaceholderValues, n1qlParams);
    } else {
      query = N1qlQuery.simple(statement, n1qlParams);
    }
    return query;
  }


}
