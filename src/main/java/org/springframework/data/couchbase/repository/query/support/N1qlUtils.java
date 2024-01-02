/*
 * Copyright 2012-2024 the original author or authors
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

package org.springframework.data.couchbase.repository.query.support;

import static org.springframework.data.couchbase.core.query.N1QLExpression.count;
import static org.springframework.data.couchbase.core.query.N1QLExpression.i;
import static org.springframework.data.couchbase.core.query.N1QLExpression.meta;
import static org.springframework.data.couchbase.core.query.N1QLExpression.path;
import static org.springframework.data.couchbase.core.query.N1QLExpression.s;
import static org.springframework.data.couchbase.core.query.N1QLExpression.select;
import static org.springframework.data.couchbase.core.query.N1QLExpression.x;
import static org.springframework.data.couchbase.core.support.TemplateUtils.SELECT_CAS;
import static org.springframework.data.couchbase.core.support.TemplateUtils.SELECT_ID;

import java.util.ArrayList;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.Field;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.CountFragment;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.Alias;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.core.EntityMetadata;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.util.TypeInformation;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Utility class to deal with constructing well formed N1QL queries around Spring Data entities, so that the framework
 * can use N1QL to find such entities (eg. restrict the bucket search to a particular type).
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 * @author Michael Reiche
 */
public class N1qlUtils {

	/**
	 * A converter that can be used to extract the {@link CouchbasePersistentProperty#getFieldName() fieldName}, eg. when
	 * one wants a path from {@link PersistentPropertyPath#toDotPath(Converter)} made of escaped field names.
	 */
	public static final Converter<? super CouchbasePersistentProperty, String> FIELD_NAME_ESCAPED = new Converter<CouchbasePersistentProperty, String>() {
		@Override
		public String convert(CouchbasePersistentProperty source) {
			return "`" + source.getFieldName() + "`";
		}
	};

	/**
	 * Escape the given bucketName and produce an {@link N1QLExpression}.
	 */
	public static N1QLExpression escapedBucket(String bucketName) {
		return i(bucketName);
	}

	/**
	 * Produce a {@link N1QLExpression} that corresponds to the SELECT clause for looking for Spring Data entities stored
	 * in Couchbase. Notably it will select the content of the document AND its id and cas and use custom construction of
	 * query if required.
	 *
	 * @param bucketName the bucket that stores the entity documents (will be escaped).
	 * @param returnedType Returned type projection information from result processor.
	 * @param converter couchbase converter
	 * @return the needed SELECT clause of the statement.
	 */
	public static N1QLExpression createSelectClauseForEntity(String bucketName, ReturnedType returnedType,
			CouchbaseConverter converter) {
		N1QLExpression bucket = escapedBucket(bucketName);
		N1QLExpression metaId = path(meta(bucket), "id").as(x(SELECT_ID));
		N1QLExpression metaCas = path(meta(bucket), "cas").as(x(SELECT_CAS));
		List<N1QLExpression> expList = new ArrayList<>();
		expList.add(metaId);
		expList.add(metaCas);

		if (returnedType != null && returnedType.needsCustomConstruction()) {
			List<String> properties = returnedType.getInputProperties();
			CouchbasePersistentEntity<?> entity = converter.getMappingContext()
					.getRequiredPersistentEntity(returnedType.getDomainType());

			for (String property : properties) {
				expList.add(path(bucket, i(entity.getRequiredPersistentProperty(property).getFieldName())));
			}
		} else {
			expList.add(path(bucket, "*"));
		}

		N1QLExpression[] propertiesExp = new N1QLExpression[expList.size()];
		propertiesExp = expList.toArray(propertiesExp);

		return select(propertiesExp);
	}

	/**
	 * Creates the returning clause for N1ql deletes with all attributes of the entity and meta information
	 *
	 * @param bucketName the bucket that stores the entity documents (will be escaped).
	 * @return the needed returning clause of the statement.
	 */
	public static N1QLExpression createReturningExpressionForDelete(String bucketName) {
		N1QLExpression fullEntity = path(i(bucketName), "*");
		N1QLExpression metaId = path(meta(i(bucketName)), "id").as(x(SELECT_ID));
		N1QLExpression metaCas = path(meta(i(bucketName)), "cas").as(x(SELECT_CAS));
		List<N1QLExpression> expList = new ArrayList<>();
		expList.add(fullEntity);
		expList.add(metaId);
		expList.add(metaCas);

		StringBuilder sb = new StringBuilder();
		for (N1QLExpression exp : expList) {
			if (sb.length() != 0) {
				sb.append(", ");
			}
			sb.append(exp.toString());
		}

		return x(sb.toString());
	}

	/**
	 * Produce a {@link N1QLExpression} that corresponds to the SELECT clause for looking for Spring Data entities stored
	 * in Couchbase. Notably it will select the content of the document AND its id and cas.
	 *
	 * @param bucketName the bucket that stores the entity documents (will be escaped).
	 * @return the needed SELECT clause of the statement.
	 */
	public static N1QLExpression createSelectClauseForEntity(String bucketName) {
		return createSelectClauseForEntity(bucketName, null, null);
	}

	/**
	 * Produce a {@link N1QLExpression} that corresponds to the SELECT...FROM clauses for looking for Spring Data entities
	 * stored in Couchbase. Notably it will select the content of the document AND its id and cas FROM the given bucket.
	 *
	 * @param bucketName the bucket that stores the entity documents (will be escaped).
	 * @return the needed SELECT...FROM clauses of the statement.
	 */
	public static N1QLExpression createSelectFromForEntity(String bucketName) {
		return createSelectClauseForEntity(bucketName).from(bucketName);
	}

	/**
	 * Produces an {@link N1QLExpression} that can serve as a WHERE clause criteria to only select documents in a bucket
	 * that matches a particular Spring Data entity (as given by the {@link EntityMetadata} parameter).
	 *
	 * @param baseWhereCriteria the other criteria of the WHERE clause, or null if none.
	 * @param converter the {@link CouchbaseConverter} giving the attribute storing the type information can be extracted.
	 * @param entityInformation the expected type information.
	 * @return an {@link N1QLExpression} to be used as a WHERE clause, that additionally restricts on the given type.
	 */
	public static N1QLExpression createWhereFilterForEntity(N1QLExpression baseWhereCriteria,
			CouchbaseConverter converter, EntityMetadata<?> entityInformation) {
		// add part that filters on type key
		String typeKey = converter.getTypeKey();
		String typeValue = entityInformation.getJavaType().getName();
        Alias alias = converter.getTypeAlias(TypeInformation.of(entityInformation.getJavaType()));
        if (alias != null && alias.isPresent()) {
            typeValue = alias.toString();
        }
        N1QLExpression typeSelector = !empty(typeKey) && !empty(typeValue) ? i(typeKey).eq(s(typeValue)) : null;
		if (baseWhereCriteria == null) {
			baseWhereCriteria = typeSelector;
        } else if (typeSelector != null) {
			baseWhereCriteria = x("(" + baseWhereCriteria.toString() + ")").and(typeSelector);
		}
		return baseWhereCriteria;
	}

    private static boolean empty(String s) {
        return s == null || s.length() == 0;
    }

	/**
	 * Given a common {@link PropertyPath}, returns the corresponding {@link PersistentPropertyPath} of
	 * {@link CouchbasePersistentProperty} which will allow to discover alternative naming for fields.
	 */
	public static PersistentPropertyPath<CouchbasePersistentProperty> getPathWithAlternativeFieldNames(
			CouchbaseConverter converter, PropertyPath property) {
		PersistentPropertyPath<CouchbasePersistentProperty> path = converter.getMappingContext()
				.getPersistentPropertyPath(property);
		return path;
	}

	/**
	 * Given a {@link PersistentPropertyPath} of {@link CouchbasePersistentProperty} (see
	 * {@link #getPathWithAlternativeFieldNames(CouchbaseConverter, PropertyPath)}), obtain a String representation of the
	 * path, separated with dots and using alternative field names.
	 */
	public static String getDottedPathWithAlternativeFieldNames(
			PersistentPropertyPath<CouchbasePersistentProperty> path) {
		return path.toDotPath(FIELD_NAME_ESCAPED);
	}

	/**
	 * Create a N1QL {@link N1QLExpression} out of a Spring Data {@link Sort}. Note that the later must use alternative
	 * field names as declared by the {@link Field} annotation on the entity, if any.
	 */
	public static N1QLExpression[] createSort(Sort sort) {
		List<N1QLExpression> cbSortList = new ArrayList<>();
		for (Sort.Order order : sort) {
			String orderProperty = order.getProperty();
			// FIXME the order property should be converted to its corresponding fieldName
			String[] orderPropertyParts = orderProperty.split("\\.");

			StringBuilder sb = new StringBuilder();
			for (String part : orderPropertyParts) {
				if (sb.length() != 0) {
					sb.append(".");
				}
				sb.append(i(part).toString());
			}
			N1QLExpression orderFieldName = x(sb.toString());
			if (order.isIgnoreCase()) {
				orderFieldName = orderFieldName.convertToString().lower();
			}
			if (order.isAscending()) {
				cbSortList.add(orderFieldName.asc());
			} else {
				cbSortList.add(orderFieldName.desc());
			}
		}
		return cbSortList.toArray(new N1QLExpression[cbSortList.size()]);
	}

	/**
	 * Creates a full N1QL query that counts total number of the given entity in the bucket.
	 *
	 * @param bucketName the name of the bucket where data is stored (will be escaped).
	 * @param converter the {@link CouchbaseConverter} giving the attribute storing the type information can be extracted.
	 * @param entityInformation the counted entity type.
	 * @return the N1QL query that counts number of documents matching this entity type.
	 */
	public static <T> N1QLExpression createCountQueryForEntity(String bucketName, CouchbaseConverter converter,
			CouchbaseEntityInformation<T, String> entityInformation) {
        N1QLExpression entityFilter = createWhereFilterForEntity(null, converter, entityInformation);
        N1QLExpression expression = select(
                (count(x("*")).as(x(CountFragment.COUNT_ALIAS))).from(escapedBucket(bucketName)));
        if (entityFilter == null) {
            return expression;
        }
        return expression.where(entityFilter);
	}

	/**
	 * Creates N1QLQuery object from the statement, query placeholder values and scan consistency
	 *
	 * @param expression A {@link N1QLExpression} representing the query to execute
	 * @param queryPlaceholderValues The positional or named parameters needed for the query
	 * @param scanConsistency The {@link QueryScanConsistency} to be used.
	 * @return A {@link N1QLQuery} to be executed.
	 */
	public static N1QLQuery buildQuery(N1QLExpression expression, JsonValue queryPlaceholderValues,
			QueryScanConsistency scanConsistency) {
		QueryOptions opts = QueryOptions.queryOptions().scanConsistency(scanConsistency);

		// put the placeholders in the options
		if (queryPlaceholderValues instanceof JsonObject && !((JsonObject) queryPlaceholderValues).isEmpty()) {
			opts.parameters((JsonObject) queryPlaceholderValues);
		} else if (queryPlaceholderValues instanceof JsonArray && !((JsonArray) queryPlaceholderValues).isEmpty()) {
			opts.parameters((JsonArray) queryPlaceholderValues);
		}
		return new N1QLQuery(expression, opts);
	}

}
