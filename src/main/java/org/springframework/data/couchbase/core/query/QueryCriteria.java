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
import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;

import org.springframework.lang.Nullable;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class QueryCriteria implements QueryCriteriaDefinition {

	private final String key;
	/**
	 * Holds the chain itself, the current operator being always the last one.
	 */
	private List<QueryCriteria> criteriaChain;
	/**
	 * Represents how the chain is hung together, null only for the first element.
	 */
	private ChainOperator chainOperator;
	private String operator;
	private Object[] value;
	private String format;

	QueryCriteria(List<QueryCriteria> chain, String key, Object[] value, ChainOperator chainOperator) {
		this(chain, key, value, chainOperator, null, null);
	}

	QueryCriteria(List<QueryCriteria> chain, String key, Object value, ChainOperator chainOperator) {
		this(chain, key, new Object[] { value }, chainOperator, null, null);
	}

	QueryCriteria(List<QueryCriteria> chain, String key, Object[] value, ChainOperator chainOperator, String operator,
			String format) {
		this.criteriaChain = chain;
		criteriaChain.add(this);
		this.key = key;
		this.value = value;
		this.chainOperator = chainOperator;
		this.operator = operator;
		this.format = format;
	}

	/**
	 * Static factory method to create a Criteria using the provided key.
	 */
	public static QueryCriteria where(String key) {
		return new QueryCriteria(new ArrayList<>(), key, null, null);
	}

	private static QueryCriteria wrap(QueryCriteria criteria) {
		QueryCriteria qc = new QueryCriteria(new LinkedList<QueryCriteria>(), criteria.key, criteria.value, null,
				criteria.operator, criteria.format);
		return qc;
	}

	public QueryCriteria and(String key) {
		return new QueryCriteria(this.criteriaChain, key, null, ChainOperator.AND);
	}

	public QueryCriteria and(QueryCriteria criteria) {
		return new QueryCriteria(this.criteriaChain, null, criteria, ChainOperator.AND);
	}

	public QueryCriteria or(QueryCriteria criteria) {
		return new QueryCriteria(this.criteriaChain, null, criteria, ChainOperator.OR);
	}

	public QueryCriteria or(String key) {
		return new QueryCriteria(this.criteriaChain, key, null, ChainOperator.OR);
	}

	public QueryCriteria eq(@Nullable Object o) {
		return is(o);
	}

	public QueryCriteria is(@Nullable Object o) {
		operator = "=";
		value = new Object[] { o };
		return this;
	}

	public QueryCriteria ne(@Nullable Object o) {
		operator = "!=";
		value = new Object[] { o };
		return this;
	}

	public QueryCriteria lt(@Nullable Object o) {
		operator = "<";
		value = new Object[] { o };
		return this;
	}

	public QueryCriteria lte(@Nullable Object o) {
		operator = "<=";
		value = new Object[] { o };
		return this;
	}

	public QueryCriteria gt(@Nullable Object o) {
		operator = ">";
		value = new Object[] { o };
		return this;
	}

	public QueryCriteria gte(@Nullable Object o) {
		operator = ">=";
		value = new Object[] { o };
		return this;
	}

	public QueryCriteria startingWith(@Nullable Object o) {
		operator = "STARTING_WITH";
		value = new Object[] { o };
		format = "%1$s like (%3$s||\"%%\")";
		return this;
	}

	public QueryCriteria plus(@Nullable Object o) {
		operator = "PLUS";
		value = new Object[] { o };
		format = "(%1$s || %3$s)";
		return this;
	}

	public QueryCriteria endingWith(@Nullable Object o) {
		operator = "ENDING_WITH";
		value = new Object[] { o };
		format = "%1$s like (\"%%\"||%3$s)";
		return this;
	}

	public QueryCriteria regex(@Nullable Object o) {
		operator = "REGEXP_LIKE";
		value = new Object[] { o };
		format = "regexp_like(%1$s, %3$s)";
		return this;
	}

	public QueryCriteria containing(@Nullable Object o) {
		operator = "CONTAINS";
		value = new Object[] { o };
		format = "contains(%1$s, %3$s)";
		return this;
	}

	public QueryCriteria notContaining(@Nullable Object o) {
		value = new QueryCriteria[] { wrap(containing(o)) };
		operator = "NOT";
		format = format = "not( %3$s )";
		return this;
	}

	public QueryCriteria like(@Nullable Object o) {
		operator = "LIKE";
		value = new Object[] { o };
		format = "%1$s like %3$s";
		return this;
	}

	public QueryCriteria notLike(@Nullable Object o) {
		operator = "NOTLIKE";
		value = new Object[] { o };
		format = "not(%1$s like %3$s)";
		return this;
	}

	public QueryCriteria isNull() {
		operator = "IS_NULL";
		value = null;
		format = "%1$s is null";
		return this;
	}

	public QueryCriteria isNotNull() {
		operator = "IS_NOT_NULL";
		value = null;
		format = "%1$s is not null";
		return (QueryCriteria) this;
	}

	public QueryCriteria isMissing() {
		operator = "IS_MISSING";
		value = null;
		format = "%1$s is missing";
		return this;
	}

	public QueryCriteria isNotMissing() {
		operator = "IS_NOT_MiSSING";
		value = null;
		format = "%1$s is not missing";
		return (QueryCriteria) this;
	}

	public QueryCriteria isValued() {
		operator = "IS_VALUED";
		value = null;
		format = "%1$s is valued";
		return this;
	}

	public QueryCriteria isNotValued() {
		operator = "IS_NOT_VALUED";
		value = null;
		format = "%1$s is not valued";
		return (QueryCriteria) this;
	}

	public QueryCriteria within(@Nullable Object o) {
		operator = "WITHIN";
		value = new Object[] { o };
		format = "%1$s within $3$s";
		return (QueryCriteria) this;
	}

	public QueryCriteria between(@Nullable Object o1, @Nullable Object o2) {
		operator = "BETWEEN";
		value = new Object[] { o1, o2 };
		format = "%1$s between %3$s and %4$s";
		return (QueryCriteria) this;
	}

	public QueryCriteria in(@Nullable Object... o) {
		operator = "IN";
		value = o;
		StringBuilder sb = new StringBuilder("%1$s in ( [ ");
		for (int i = 1; i <= value.length; i++) { // format indices start at 1
			if (i > 1)
				sb.append(", ");
			sb.append("%" + (i + 2) + "$s"); // the first is fieldName, second is operator, args start at 3
		}
		format = sb.append(" ] )").toString();
		return (QueryCriteria) this;
	}

	public QueryCriteria notIn(@Nullable Object... o) {
		value = new QueryCriteria[] { wrap(in(o)) };
		operator = "NOT";
		format = format = "not( %3$s )"; // field = 1$, operator = 2$, value=$3, $4, ...
		return (QueryCriteria) this;
	}

	public QueryCriteria TRUE() { // true/false are reserved, use TRUE/FALSE
		value = null;
		operator = null;
		format = format = "%1$s"; // field = 1$, operator = 2$, value=$3, $4, ...
		return (QueryCriteria) this;
	}

	public QueryCriteria FALSE() {
		value = new QueryCriteria[] { wrap(TRUE()) };
		operator = "not";
		format = format = "not( %3$s )";
		return (QueryCriteria) this;
	}

	/**
	 * This exports the query criteria into a string to be appended to the beginning of an N1QL statement
	 *
	 * @param paramIndexPtr - this is a reference to the parameter index to be used for positional parameters
	 *                      There may already be positional parameters in the beginning of the statement,
	 *                      so it may not always start at 1.  If it has the value -1, the query is using
	 *                      named parameters. If the pointer is null, the query is not using parameters.
	 * @return string containing part of N1QL query
	 */
	@Override
	public String export(int[] paramIndexPtr) {
		StringBuilder output = new StringBuilder();
		boolean first = true;
		for (QueryCriteria c : this.criteriaChain) {
			if (!first) {
				if (c.chainOperator == null) {
					throw new IllegalStateException("A chain operator must be present when chaining!");
				}
				// the consistent place to output this would be in the c.exportSingle(output) about five lines down
				output.append(" ").append(c.chainOperator.representation).append(" ");
			} else {
				first = false;
			}
			c.exportSingle(output, paramIndexPtr);
		}

		return output.toString();
	}

	/**
	 * Export the query criteria to a string without using positional or named parameters.
	 *
	 * @return string containing part of N1QL query
	 */
	@Override
	public String export() {
		return export(null);

	}

	private StringBuilder exportSingle(StringBuilder sb, int[] paramIndexPtr) {
		String fieldName = maybeQuote(key);
		int valueLen = value == null ? 0 : value.length;
		Object[] v = new Object[valueLen + 2];
		v[0] = fieldName;
		v[1] = operator;
		for (int i = 0; i < valueLen; i++) {
			if (value[i] instanceof QueryCriteria) {
				v[i + 2] = "(" + ((QueryCriteria) value[i]).export(paramIndexPtr) + ")";
			} else {
				v[i + 2] = maybeWrapValue(key, value[i], paramIndexPtr);
			}
		}

		if (key == null) { // chaining, the chainingOperator was already output by export()
			sb.append(v[2]);
		} else if (format == null) { // this always has to be fieldname <op> <something>
			sb.append(fieldName).append(" ").append(operator).append(" ").append(v[2]);
		} else {
			sb.append(new Formatter().format(format, v));
		}

		return sb;
	}

	private String maybeWrapValue(String key, Object value, int[] paramIndexPtr) {
		if (paramIndexPtr != null) {
			if (paramIndexPtr[0] >= 0) {
				return "$" + (++paramIndexPtr[0]); // these are generated in order
			} else {
				return "$" + key;
			}
		}

		if (value instanceof String) {
			return "\"" + value + "\"";
		} else if (value == null) {
			return "null";
		} else {
			return value.toString();
		}
	}

	private String maybeQuote(String value) {
		if (value == null || (value.startsWith("\"") && value.endsWith("\""))) {
			return value;
		} else {
			return "`" + value + "`";
		}
	}

	enum ChainOperator {
		AND("and"), OR("or");

		private final String representation;

		ChainOperator(String representation) {
			this.representation = representation;
		}

		public String getRepresentation() {
			return representation;
		}
	}

}
