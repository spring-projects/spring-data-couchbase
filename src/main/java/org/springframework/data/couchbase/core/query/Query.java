/*
 * Copyright 2012-2025 the original author or authors
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
package org.springframework.data.couchbase.core.query;

import static org.springframework.util.Assert.notNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.repository.query.CouchbaseQueryMethod;
import org.springframework.data.couchbase.repository.query.StringBasedN1qlQueryParser;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.Alias;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class Query {

	private final List<QueryCriteriaDefinition> criteria = new ArrayList<>();
	private JsonValue parameters = JsonValue.ja();
	private long skip;
	private int limit;
	private boolean distinct;
	private String[] distinctFields;
	protected Sort sort = Sort.unsorted();
	private QueryScanConsistency queryScanConsistency;
	private Meta meta;

	static private final Pattern WHERE_PATTERN = Pattern.compile("\\sWHERE\\s");
	private static final Logger LOG = LoggerFactory.getLogger(Query.class);

	public Query() {}

	public Query(final QueryCriteriaDefinition criteriaDefinition) {
		addCriteria(criteriaDefinition);
	}

	public Query(Query that) {
		Assert.notNull(that, "source query cannot be null");
		this.criteria.addAll(that.criteria);
		this.parameters = that.parameters;
		this.skip = that.skip;
		this.limit = that.limit;
		this.distinct = that.distinct;
		this.distinctFields = that.distinctFields;
		this.sort = that.sort;
		this.queryScanConsistency = that.queryScanConsistency;
		this.meta = that.meta;
	};

	public static Query query(QueryCriteriaDefinition criteriaDefinition) {
		return new Query(criteriaDefinition);
	}

	public Query addCriteria(QueryCriteriaDefinition criteriaDefinition) {
		this.criteria.add(criteriaDefinition);
		return this;
	}

	protected List<QueryCriteriaDefinition> getCriteriaList() {
		return this.criteria;
	}

	/**
	 * set the postional parameters on the query object There can only be named parameters or positional parameters - not
	 * both.
	 *
	 * @param parameters - the positional parameters
	 * @return - the query
	 */
	public Query setPositionalParameters(JsonArray parameters) {
		this.parameters = parameters;
		return this;
	}

	/**
	 * set the named parameters on the query object There can only be named parameters or positional parameters - not
	 * both.
	 *
	 * @param parameters - the named parameters
	 * @return - the query
	 */
	public Query setNamedParameters(JsonObject parameters) {
		this.parameters = parameters;
		return this;
	}

	JsonValue getParameters() {
		return parameters;
	}

	/**
	 * Set number of documents to skip before returning results.
	 *
	 * @param skip
	 * @return
	 */
	public Query skip(long skip) {
		this.skip = skip;
		return this;
	}

	/**
	 * Limit the number of returned documents to {@code limit}.
	 *
	 * @param limit
	 * @return
	 */
	public Query limit(int limit) {
		this.limit = limit;
		return this;
	}

	/**
	 * Is this a DISTINCT query? {@code distinct}.
	 *
	 * @param distinct
	 * @return
	 */
	public Query distinct(boolean distinct) {
		this.distinct = distinct;
		return this;
	}

	/**
	 * Is this a DISTINCT query? {@code distinct}.
	 *
	 * @return distinct
	 */
	public boolean isDistinct() {
		return distinct;
	}

	/**
	 * distinctFields for query (non-null but empty means all fields) ? {@code distinctFields}.
	 *
	 * @param distinctFields
	 * @return
	 */
	public Query distinct(String[] distinctFields) {
		this.distinctFields = distinctFields;
		return this;
	}

	/**
	 * distinctFields for query (non-null but empty means all fields) ? {@code distinctFields}.
	 *
	 * @return distinctFields
	 */
	public String[] getDistinctFields() {
		return distinctFields;
	}

	/**
	 * Sets the given pagination information on the {@link Query} instance. Will transparently set {@code skip} and
	 * {@code limit} as well as applying the {@link Sort} instance defined with the {@link Pageable}.
	 *
	 * @param pageable
	 * @return
	 */
	public Query with(final Pageable pageable) {
		if (pageable.isUnpaged()) {
			return this;
		}
		this.limit = pageable.getPageSize();
		this.skip = pageable.getOffset();
		this.with(pageable.getSort());
		return this;
	}

	/**
	 * queryScanConsistency
	 *
	 * @return queryScanConsistency
	 */
	public QueryScanConsistency getScanConsistency() {
		return queryScanConsistency;
	}

	/**
	 * Sets the given scan consistency on the {@link Query} instance.
	 *
	 * @param queryScanConsistency
	 * @return this
	 */
	public Query scanConsistency(final QueryScanConsistency queryScanConsistency) {
		this.queryScanConsistency = queryScanConsistency;
		return this;
	}

	/**
	 * Adds a {@link Sort} to the {@link Query} instance.
	 *
	 * @param sort
	 * @return
	 */
	public Query with(final Sort sort) {
		notNull(sort, "Sort must not be null!");
		if (sort.isUnsorted()) {
			return this;
		}
		this.sort = this.sort.and(sort);
		return this;
	}

	public Query withoutSort() {
		this.sort = Sort.unsorted();
		return this;
	}

	public void appendSkipAndLimit(final StringBuilder sb) {
		if (limit > 0) {
			sb.append(" LIMIT ").append(limit);
		}
		if (skip > 0) {
			sb.append(" OFFSET ").append(skip);
		}
	}

	public void appendSort(final StringBuilder sb) {
		if (sort.isUnsorted()) {
			return;
		}

		sb.append(" ORDER BY ");
		sort.stream().forEach(order -> {
			if (order.isIgnoreCase()) {
				throw new IllegalArgumentException(String.format("Given sort contained an Order for %s with ignore case! "
						+ "Couchbase N1QL does not support sorting ignoring case currently!", order.getProperty()));
			}
			sb.append(order.getProperty()).append(" ").append(order.isAscending() ? "ASC," : "DESC,");
		});
		sb.deleteCharAt(sb.length() - 1);
	}

	public void appendWhere(final StringBuilder sb, int[] paramIndexPtr, CouchbaseConverter converter) {
		if (!criteria.isEmpty()) {
			appendWhereOrAnd(sb);
			boolean first = true;
			for (QueryCriteriaDefinition c : criteria) {
				if (first) {
					first = false;
				} else {
					sb.append(" AND ");
				}
				sb.append(c.export(paramIndexPtr, parameters, converter));
			}
		}
	}

	public void appendWhereString(StringBuilder sb, String whereString) {
		appendWhereOrAnd(sb);
		sb.append(whereString);
	}

	public void appendString(StringBuilder sb, String whereString) {
		sb.append(whereString);
	}

	private void appendWhereOrAnd(StringBuilder sb) {
		String querySoFar = sb.toString().toUpperCase();
		Matcher whereMatcher = WHERE_PATTERN.matcher(querySoFar);
		boolean alreadyWhere = false;
		while (!alreadyWhere && whereMatcher.find()) {
			if (notQuoted(whereMatcher.start(), whereMatcher.end(), querySoFar)) {
				alreadyWhere = true;
			}
		}
		if (alreadyWhere) {
			sb.append(" AND ");
		} else {
			sb.append(" WHERE ");
		}
	}

	/**
	 * ensure that the WHERE we found was not quoted
	 *
	 * @param start
	 * @param end
	 * @param querySoFar
	 * @return true -> not quoted, false -> quoted
	 */
	private static boolean notQuoted(int start, int end, String querySoFar) {
		Matcher quoteMatcher = StringBasedN1qlQueryParser.QUOTE_DETECTION_PATTERN.matcher(querySoFar);
		List<int[]> quotes = new ArrayList<int[]>();
		while (quoteMatcher.find()) {
			quotes.add(new int[] { quoteMatcher.start(), quoteMatcher.end() });
		}

		for (int[] quote : quotes) {
			if (quote[0] <= start && quote[1] >= end) {
				return false; // it is quoted
			}
		}
		return true; // is not quoted
	}

	public String export(int[]... paramIndexPtrHolder) { // used only by tests
		StringBuilder sb = new StringBuilder();
		appendWhere(sb, paramIndexPtrHolder.length > 0 ? paramIndexPtrHolder[0] : null, null);
		appendSort(sb);
		appendSkipAndLimit(sb);
		return sb.toString();
	}

	/**
	 *
	 */
	@Deprecated
	public String toN1qlSelectString(ReactiveCouchbaseTemplate template, Class domainClass, boolean isCount) {
		return toN1qlSelectString(template.getConverter(), template.getBucketName(), null, null, domainClass, null, isCount,
				null, null);
	}

	public String toN1qlSelectString(CouchbaseConverter converter, String bucketName, String scopeName,
			String collectionName, Class domainClass, Class returnClass, boolean isCount, String[] distinctFields,
			String[] fields) {
		StringBasedN1qlQueryParser.N1qlSpelValues n1ql = getN1qlSpelValues(converter, bucketName, scopeName, collectionName,
				domainClass, returnClass, isCount, distinctFields, fields);
		final StringBuilder statement = new StringBuilder();
		appendString(statement, n1ql.selectEntity); // select ...
        if (n1ql.filter != null) {
            appendWhereString(statement, n1ql.filter); // typeKey = typeValue
        }
		appendWhere(statement, new int[] { 0 }, converter); // criteria on this Query
		if (!isCount) {
			appendSort(statement);
			appendSkipAndLimit(statement);
		}
		return statement.toString();
	}

	public String toN1qlRemoveString(CouchbaseConverter converter, String bucketName, String scopeName,
			String collectionName, Class domainClass) {
		StringBasedN1qlQueryParser.N1qlSpelValues n1ql = getN1qlSpelValues(converter, bucketName, scopeName, collectionName,
				domainClass, null, false, null, null);
		final StringBuilder statement = new StringBuilder();
		appendString(statement, n1ql.delete); // delete ...
        if (n1ql.filter != null) {
            appendWhereString(statement, n1ql.filter); // typeKey = typeValue
        }
		appendWhere(statement, null, converter); // criteria on this Query
		appendString(statement, n1ql.returning);
		return statement.toString();
	}

	public static StringBasedN1qlQueryParser.N1qlSpelValues getN1qlSpelValues(CouchbaseConverter converter,
			String bucketName, String scopeName, String collectionName, Class domainClass, Class returnClass, boolean isCount,
			String[] distinctFields, String[] fields) {
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

		StringBasedN1qlQueryParser sbnqp = new StringBasedN1qlQueryParser(bucketName, scopeName, collectionName, converter,
				domainClass, returnClass, typeKey, typeValue, isCount, distinctFields, fields);
		return sbnqp.getStatementContext();
	}

	/**
	 * build QueryOptions from parameters and scanConsistency
	 *
	 * @param scanConsistency
	 * @return QueryOptions
	 */
	public QueryOptions buildQueryOptions(QueryOptions options, QueryScanConsistency scanConsistency) {
		return OptionsBuilder.buildQueryOptions(this, options, scanConsistency);
	}

	/**
	 * this collections annotations from the method, repository class and possibly the entity class to be used as options.
	 * This will find annotations included in composed annotations as well. Ideally
	 * 
	 * @param method representing the query.
	 */
	public void setMeta(CouchbaseQueryMethod method, Class<?> typeToRead) {
		meta = OptionsBuilder.buildMeta(method, typeToRead);
	}

	public Meta getMeta() {
		return meta;
	}

	public boolean isReadonly() {
		return true;
	}

	public boolean equals(Object o) {
		if (!o.getClass().isAssignableFrom(getClass())) {
			return false;
		}
		Query that = (Query) o;
		if (this.criteria.size() != that.criteria.size()) {
			return false;
		}
		if (this.criteria.equals(that.criteria)) {
			return false;
		}
		int i = 0;
		for (QueryCriteriaDefinition thisCriteria : this.criteria) {
			if (!thisCriteria.equals(that.criteria.get(i))) {
				return false;
			}
		}

		if (this.parameters.equals(that.parameters)) {
			return false;
		}
		;
		if (this.skip != that.skip) {
			return false;
		}
		if (this.limit != that.limit) {
			return false;
		}
		if (this.distinct != that.distinct) {
			return false;
		}

		if (Arrays.equals(this.distinctFields, that.distinctFields)) {
			return false;
		}
		if (this.sort != that.sort) {
			return false;
		}
		if (this.queryScanConsistency != that.queryScanConsistency) {
			return false;
		}
		if (!meta.equals(that.meta)) {
			return false;
		}
		return true;
	}

}
