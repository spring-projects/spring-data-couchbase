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

package org.springframework.data.couchbase.repository.query.support;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.Expression.path;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static com.couchbase.client.java.query.dsl.functions.AggregateFunctions.count;
import static com.couchbase.client.java.query.dsl.functions.MetaFunctions.meta;
import static com.couchbase.client.java.query.dsl.functions.StringFunctions.lower;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.deps.com.fasterxml.jackson.databind.ser.std.StdArraySerializers;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.functions.TypeFunctions;
import com.couchbase.client.java.query.dsl.path.FromPath;
import com.couchbase.client.java.query.dsl.path.WherePath;
import com.couchbase.client.java.repository.annotation.Field;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.CountFragment;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.repository.core.EntityMetadata;
import org.springframework.data.repository.core.support.ExampleMatcherAccessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.util.DirectFieldAccessFallbackBeanWrapper;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

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
    Expression metaId = path(meta(bucket), "id").as(CouchbaseOperations.SELECT_ID);
    Expression metaCas = path(meta(bucket), "cas").as(CouchbaseOperations.SELECT_CAS);
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
   * Produces an {@link Expression} that can serve as a WHERE clause criteria to only select documents in a bucket
   * that matches a particular Spring Data entity (as given by the {@link EntityMetadata} parameter)
   * and an Example {@link Example}.
   *
   * @param baseWhereCriteria the other criteria of the WHERE clause, or null if none.
   * @param converter the {@link CouchbaseConverter} giving the attribute storing the type information can be extracted.
   * @param entityInformation the expected type information.
   * @param example the {@link Example} expected example to match with.
   * @return an {@link Expression} to be used as a WHERE clause, that additionally restricts on the given type.
   */
  public static Expression createWhereFilterByExampleForEntity(Expression baseWhereCriteria,
                                                                  CouchbaseConverter converter,
                                                                  EntityMetadata<?> entityInformation,
                                                                  Example example) {
    ExampleMatcher matcher = example.getMatcher();

    CouchbasePersistentEntity entity = converter.getMappingContext().getPersistentEntity(entityInformation.getJavaType());
    List<Expression> predicates = new ArrayList<Expression>();
    getPredicates(predicates, "", converter, entity, example.getProbe(), new ExampleMatcherAccessor(matcher));

    Expression whereCriteria = createWhereFilterForEntity(baseWhereCriteria, converter, entityInformation);

    for (Expression x:predicates) {
      whereCriteria = matcher.isAllMatching() ? whereCriteria.and(x) : whereCriteria.or(x);
    }
    return whereCriteria;
  }

  private static void getPredicates(final List<Expression> predicates,
                                    final String path,
                                    final CouchbaseConverter converter,
                                    final CouchbasePersistentEntity entity,
                                    final Object value,
                                    final ExampleMatcherAccessor exampleAccessor) {
    final DirectFieldAccessFallbackBeanWrapper beanWrapper = new DirectFieldAccessFallbackBeanWrapper(value);
    entity.doWithProperties(new PropertyHandler<CouchbasePersistentProperty>() {
        @Override
        public void doWithPersistentProperty(final CouchbasePersistentProperty prop) {
          String currentPath = !StringUtils.hasText(path) ? prop.getName() : path + "." + prop.getName();

          if (exampleAccessor.isIgnoredPath(currentPath)) {
            return;
          }

          Object attributeValue = exampleAccessor.getValueTransformerForPath(currentPath)
                  .convert(beanWrapper.getPropertyValue(currentPath));

          if (attributeValue == null) {
            if (exampleAccessor.getNullHandler().equals(ExampleMatcher.NullHandler.INCLUDE)) {
              predicates.add(i(currentPath).isNull());
            }
            return;
          }
          if(prop.isEntity()) {
            getPredicates(predicates, currentPath, converter, converter.getMappingContext().getPersistentEntity(prop.getType()),
                    value, exampleAccessor);
            return;
          }

          Expression escapedPath = null;
          if (currentPath.indexOf(".") == -1) {
            escapedPath = i(currentPath);
          } else {
            String[] parts = currentPath.split("\\.");
            StringBuilder stringBuilder = new StringBuilder();
            for(int i = 0; i < parts.length; i++) {
              stringBuilder.append(i(parts[i]).toString());
              if (i != parts.length -1) {
                stringBuilder.append(".");
              }
            }
            escapedPath = x(stringBuilder.toString());
          }

          if (prop.getActualType().equals(String.class)) {
            if (exampleAccessor.isIgnoreCaseForPath(currentPath)) {
              attributeValue = attributeValue.toString().toLowerCase();
            }

            switch (exampleAccessor.getStringMatcherForPath(currentPath)) {
              case DEFAULT:
              case EXACT:
                predicates.add(escapedPath.eq(s(attributeValue.toString())));
                break;
              case CONTAINING:
                predicates.add(escapedPath.like(s("%" + attributeValue.toString() + "%")));
                break;
              case STARTING:
                predicates.add(escapedPath.like(s(attributeValue.toString() + "%")));
                break;
              case ENDING:
                predicates.add(escapedPath.like(s("%" + attributeValue.toString())));
                break;
              default:
                throw new IllegalArgumentException(
                        "Unsupported StringMatcher " + exampleAccessor.getStringMatcherForPath(currentPath));
            }
          } else if (prop.getActualType().equals(int.class)) {
            predicates.add(escapedPath.eq(Integer.parseInt(attributeValue.toString())));
          } else if (prop.getActualType().equals(long.class)) {
            predicates.add(escapedPath.eq(Long.parseLong(attributeValue.toString())));
          } else if (prop.getActualType().equals(double.class)) {
              predicates.add(escapedPath.eq(Double.parseDouble(attributeValue.toString())));
          } else if (prop.getActualType().equals(float.class)) {
              predicates.add(escapedPath.eq(Float.parseFloat(attributeValue.toString())));
          } else if (prop.getActualType().equals(boolean.class)) {
              predicates.add(escapedPath.eq(Boolean.parseBoolean(attributeValue.toString())));
          }
        }
    });

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
   * Creates a full N1QL query that counts total number of the given entity in the bucket matching the given example.
   *
   * @param bucketName the name of the bucket where data is stored (will be escaped).
   * @param converter the {@link CouchbaseConverter} giving the attribute storing the type information can be extracted.
   * @param entityInformation the counted entity type.
   * @param example {@link Example} example to match with
   * @return the N1QL query that counts number of documents matching this entity type and example.
   */
  public static <T> Statement createCountQueryByExampleForEntity(String bucketName, CouchbaseConverter converter, CouchbaseEntityInformation<T, String> entityInformation, Example example) {
	  return select(count("*").as(CountFragment.COUNT_ALIAS)).from(escapedBucket(bucketName)).where(createWhereFilterByExampleForEntity(null, converter, entityInformation, example));
  }

}
