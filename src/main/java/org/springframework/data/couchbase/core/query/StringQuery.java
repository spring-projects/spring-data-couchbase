/*
 * Copyright 2017-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core.query;

import java.util.Locale;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.support.TemplateUtils;
import org.springframework.data.couchbase.repository.query.CouchbaseQueryMethod;
import org.springframework.data.couchbase.repository.query.StringBasedN1qlQueryParser;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;
import org.springframework.data.mapping.Alias;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;

/**
 * Query created from the string in @Query annotation in the repository interface.
 * 
 * <pre>
 * &#64;Query("#{#n1ql.selectEntity} where #{#n1ql.filter} and firstname = $1 and lastname = $2")
 * List&lt;User&gt; getByFirstnameAndLastname(String firstname, String lastname);
 * </pre>
 * 
 * It must include the SELECT ... FROM ... preferably via the #n1ql expression, in addition to any predicates required,
 * including the n1ql.filter (for _class = className)
 * 
 * @author Michael Reiche
 */
public class StringQuery extends Query {

	private final CouchbaseQueryMethod queryMethod;
	private final String inlineN1qlQuery;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;
	private final ParameterAccessor parameterAccessor;
	private final SpelExpressionParser spelExpressionParser;

	public StringQuery(CouchbaseQueryMethod queryMethod, String n1qlString,
			QueryMethodEvaluationContextProvider queryMethodEvaluationContextProvider, ParameterAccessor parameterAccessor,
			SpelExpressionParser spelExpressionParser) {
		this.queryMethod = queryMethod;
		this.inlineN1qlQuery = n1qlString;
		this.evaluationContextProvider = queryMethodEvaluationContextProvider;
		this.parameterAccessor = parameterAccessor;
		this.spelExpressionParser = spelExpressionParser;
	}

	public StringQuery(String n1qlString) {
		this(null,n1qlString, null, null, null);
	}

	@Override
	public String toN1qlSelectString(CouchbaseConverter converter, String bucketName, String scope, String collection,
			Class domainClass, Class resultClass, boolean isCount, String[] distinctFields, String[] fields) {

		StringBasedN1qlQueryParser parser = getStringN1qlQueryParser(converter, bucketName, scope, collection, domainClass,
				distinctFields, fields);

		N1QLExpression parsedExpression = parser.getExpression(inlineN1qlQuery, queryMethod, parameterAccessor,
				spelExpressionParser, evaluationContextProvider);

		String queryString = parsedExpression.toString();

		JsonValue parameters = parser.getPlaceholderValues(parameterAccessor);
		if (parameters instanceof JsonArray) {
			this.setPositionalParameters((JsonArray) parameters);
		} else {
			this.setNamedParameters((JsonObject) parameters);
		}
		final StringBuilder statement = new StringBuilder();
		boolean makeCount = isCount && queryString != null && !queryString.toLowerCase(Locale.ROOT).contains("count(");
		if (makeCount) {
			statement.append("SELECT COUNT(*) AS " + TemplateUtils.SELECT_COUNT + " FROM (");
		}
		statement.append(queryString); // apply the string statement
		// To use generated parameters for literals
		// we need to figure out if we must use positional or named parameters
		// If we are using positional parameters, we need to start where
		// the inlineN1ql left off.
		int[] paramIndexPtr = null;
		JsonValue params = this.getParameters();
		if (params instanceof JsonArray) { // positional parameters
			paramIndexPtr = new int[] { ((JsonArray) params).size() };
		} else { // named parameters or no parameters, no index required
			paramIndexPtr = new int[] { -1 };
		}
		appendWhere(statement, paramIndexPtr, converter); // criteria on this Query - should be empty for
		if (!isCount) {
			appendSort(statement);
			appendSkipAndLimit(statement);
		}
		if (makeCount) {
			statement.append(") predicate_query");
		}
		return statement.toString();
	}

	private StringBasedN1qlQueryParser getStringN1qlQueryParser(CouchbaseConverter converter, String bucketName,
			String scopeName, String collectionName, Class domainClass, String[] distinctFields, String[] fields) {
		String typeKey = converter.getTypeKey();
		final CouchbasePersistentEntity<?> persistentEntity = converter.getMappingContext()
				.getRequiredPersistentEntity(domainClass);
		MappingCouchbaseEntityInformation<?, Object> info = new MappingCouchbaseEntityInformation<>(persistentEntity);
		String typeValue = info.getJavaType().getName();
		TypeInformation<?> typeInfo = TypeInformation.of(info.getJavaType());
		Alias alias = converter.getTypeAlias(typeInfo);
		if (alias != null && alias.isPresent()) {
			typeValue = alias.toString();
		}
		// there are no options for distinct and fields for @Query
		StringBasedN1qlQueryParser sbnqp = new StringBasedN1qlQueryParser(inlineN1qlQuery, queryMethod, bucketName,
				scopeName, collectionName, converter, typeKey, typeValue, parameterAccessor, new SpelExpressionParser(),
				evaluationContextProvider);

		return sbnqp;
	}

	@Override
	public boolean isReadonly() {
		if (this.queryMethod.hasN1qlAnnotation()) {
			return this.queryMethod.getN1qlAnnotation().readonly();
		}
		return false;
	}

	/**
	 * toN1qlRemoveString - use toN1qlSelectString
	 * 
	 * @param converter
	 * @param bucketName
	 * @param scopeName
	 * @param collectionName
	 * @param domainClass
	 */
	@Override
	public String toN1qlRemoveString(CouchbaseConverter converter, String bucketName, String scopeName,
			String collectionName, Class domainClass) {
		return toN1qlSelectString(converter, bucketName, scopeName, collectionName, domainClass, domainClass, false, null,
				null);
	}
}
