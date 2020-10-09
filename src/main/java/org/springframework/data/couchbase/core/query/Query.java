/*
 * Copyright 2012-2020 the original author or authors
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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.repository.query.StringBasedN1qlQueryParser;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.Alias;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class Query {

	private final List<QueryCriteria> criteria = new ArrayList<>();
	private JsonValue parameters = JsonValue.ja();
	private long skip;
	private int limit;
	private Sort sort = Sort.unsorted();
	private QueryScanConsistency queryScanConsistency;

	static private final Pattern WHERE_PATTERN = Pattern.compile("\\sWHERE\\s");

	public Query() {}

	public Query(final QueryCriteria criteriaDefinition) {
		addCriteria(criteriaDefinition);
	}

	public Query addCriteria(QueryCriteria criteriaDefinition) {
		this.criteria.add(criteriaDefinition);
		return this;
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
		return with(pageable.getSort());
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
		Assert.notNull(sort, "Sort must not be null!");
		if (sort.isUnsorted()) {
			return this;
		}
		this.sort = this.sort.and(sort);
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
			for (QueryCriteria c : criteria) {
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

	public String toN1qlSelectString(ReactiveCouchbaseTemplate template, Class domainClass, boolean isCount) {
		StringBasedN1qlQueryParser.N1qlSpelValues n1ql = getN1qlSpelValues(template, domainClass, isCount);
		final StringBuilder statement = new StringBuilder();
		appendString(statement, n1ql.selectEntity); // select ...
		appendWhereString(statement, n1ql.filter); // typeKey = typeValue
		appendWhere(statement, new int[] { 0 }, template.getConverter()); // criteria on this Query
		appendSort(statement);
		appendSkipAndLimit(statement);
		return statement.toString();
	}

	public String toN1qlRemoveString(ReactiveCouchbaseTemplate template, Class domainClass) {
		StringBasedN1qlQueryParser.N1qlSpelValues n1ql = getN1qlSpelValues(template, domainClass, false);
		final StringBuilder statement = new StringBuilder();
		appendString(statement, n1ql.delete); // delete ...
		appendWhereString(statement, n1ql.filter); // typeKey = typeValue
		appendWhere(statement, null, template.getConverter()); // criteria on this Query
		appendString(statement, n1ql.returning);
		return statement.toString();
	}

	StringBasedN1qlQueryParser.N1qlSpelValues getN1qlSpelValues(ReactiveCouchbaseTemplate template, Class domainClass,
			boolean isCount) {
		String typeKey = template.getConverter().getTypeKey();
		final CouchbasePersistentEntity<?> persistentEntity = template.getConverter().getMappingContext()
				.getRequiredPersistentEntity(domainClass);
		MappingCouchbaseEntityInformation<?, Object> info = new MappingCouchbaseEntityInformation<>(persistentEntity);
		String typeValue = info.getJavaType().getName();
		TypeInformation<?> typeInfo = ClassTypeInformation.from(info.getJavaType());
		Alias alias = template.getConverter().getTypeAlias(typeInfo);
		if (alias != null && alias.isPresent()) {
			typeValue = alias.toString();
		}
		return StringBasedN1qlQueryParser.createN1qlSpelValues(template.getBucketName(), typeKey, typeValue, isCount);
	}

	/**
	 * build QueryOptions from parameters and scanConsistency
	 *
	 * @param scanConsistency
	 * @return QueryOptions
	 */
	public QueryOptions buildQueryOptions(QueryScanConsistency scanConsistency) {
		final QueryOptions options = QueryOptions.queryOptions();
		if (getParameters() != null) {
			if (getParameters() instanceof JsonArray) {
				options.parameters((JsonArray) getParameters());
			} else {
				options.parameters((JsonObject) getParameters());
			}
		}
		if (scanConsistency != null) {
			options.scanConsistency(scanConsistency);
		}

		return options;
	}

}
