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

import com.couchbase.client.java.json.JsonValue;
import org.springframework.data.couchbase.repository.query.StringBasedN1qlQueryParser;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.util.Assert;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class Query {

	private final List<QueryCriteria> criteria = new ArrayList<>();
	private JsonValue parameters = JsonValue.ja();
	private String inlineN1qlQuery = null;
	private long skip;
	private int limit;
	private Sort sort = Sort.unsorted();

	public Query() {
	}

	public Query(final QueryCriteria criteriaDefinition) {
		addCriteria(criteriaDefinition);
	}

	public Query addCriteria(QueryCriteria criteriaDefinition) {
		this.criteria.add(criteriaDefinition);
		return this;
	}

	public Query setParameters(JsonValue parameters) {
		this.parameters = parameters;
		return this;
	}

	public JsonValue getParameters() {
		return parameters;
	}

	public Query clearCriteria() { // if annotated with string query, ignore all criteria
		this.criteria.clear();
		return this;
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

	/**
	 * inlineN1qlQuery (Query Annotation)
	 */
	public Query setInlineN1qlQuery(String inlineN1qlQuery) {
		this.inlineN1qlQuery = inlineN1qlQuery;
		return this;
	}

	/**
	 * inlineN1qlQuery (Query Annotation)
	 */
	public String getInlineN1qlQuery() {
		return inlineN1qlQuery;
	}

	/**
	 * inlineN1qlQuery (Query Annotation)
	 */
	public boolean hasInlineN1qlQuery() {
		return inlineN1qlQuery != null;
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
				throw new IllegalArgumentException(String.format(
						"Given sort contained an Order for %s with ignore case! "
								+ "Couchbase N1QL does not support sorting ignoring case currently!",
						order.getProperty()));
			}
			sb.append(order.getProperty()).append(" ").append(order.isAscending() ? "ASC," : "DESC,");
		});
		sb.deleteCharAt(sb.length() - 1);
	}

	public void appendWhere(final StringBuilder sb, int[] paramIndexPtr) {
		if (!criteria.isEmpty()) {
			appendWhereOrAnd(sb);
			boolean first = true;
			for (QueryCriteria c : criteria) {
				if (first) {
					first = false;
				} else {
					sb.append(" AND ");
				}
				sb.append(c.export(paramIndexPtr));
			}
		}
	}

	public void appendCriteria(StringBuilder sb, QueryCriteria criteria){
		appendWhereOrAnd(sb);
		sb.append(criteria.export());
	}

	private void appendWhereOrAnd(StringBuilder sb) {

		String querySoFar = sb.toString().toUpperCase();

		Pattern WHERE_PATTERN = Pattern.compile("\\sWHERE\\s");
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

	/**
	 * inlineN1qlQuery (Query Annotation)
	 */
	public void appendInlineN1qlStatement(final StringBuilder sb) {
		sb.append(getInlineN1qlQuery());
	}

	public String export() {
		StringBuilder sb = new StringBuilder();
		appendWhere(sb,null);
		appendSort(sb);
		appendSkipAndLimit(sb);
		return sb.toString();
	}

}
