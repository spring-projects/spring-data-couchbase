/*
 * Copyright 2012-2022 the original author or authors
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

import static org.springframework.data.couchbase.core.query.N1QLExpression.x;

import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;

import com.couchbase.client.core.error.CouchbaseException;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Mauro Monti
 */
public class QueryCriteria implements QueryCriteriaDefinition {

	private N1QLExpression key;
	/**
	 * Holds the chain itself, the current operator being always the last one.
	 */
	private LinkedList<QueryCriteria> criteriaChain;
	/**
	 * Represents how the chain is hung together, null only for the first element.
	 */
	private ChainOperator chainOperator;
	private String operator;
	private Object[] value;
	private String format;

	public QueryCriteria(LinkedList<QueryCriteria> chain, N1QLExpression key, Object[] value,
			ChainOperator chainOperator) {
		this(chain, key, value, chainOperator, null, null);
	}

	QueryCriteria(LinkedList<QueryCriteria> chain, N1QLExpression key, Object value, ChainOperator chainOperator) {
		this(chain, key, new Object[] { value }, chainOperator, null, null);
	}

	QueryCriteria(LinkedList<QueryCriteria> chain, N1QLExpression key, Object[] value, ChainOperator chainOperator,
			String operator, String format) {
		this.criteriaChain = chain != null ? chain : new LinkedList<>();
		this.chainOperator = chainOperator;
		this.criteriaChain.add(this);// add the new one to the chain. The new one has the chainOperator set
		this.key = key;
		this.value = value;
		// this.chainOperator = chainOperator; ignored now.
		this.operator = operator;
		this.format = format;
	}

	/**
	 * Static factory method to create a Criteria using the provided String key.
	 */
	public static QueryCriteria where(String key) {
		return where(x(key));
	}

	/**
	 * Static factory method to create a Criteria using the provided N1QLExpression key.
	 */
	public static QueryCriteria where(N1QLExpression key) {
		return new QueryCriteria(null, key, null, null);
	}

	// wrap criteria (including the criteriaChain) in a new QueryCriteria and set the queryChain of the original criteria
	// to just itself. The new query will be the value[0] of the original query.
	private void replaceThisAsWrapperOf(QueryCriteria criteria) {
		QueryCriteria qc = new QueryCriteria(criteria.criteriaChain, criteria.key, criteria.value, criteria.chainOperator,
				criteria.operator, criteria.format);
		criteriaChain.remove(criteria); // we replaced it with the clone
		criteriaChain = new LinkedList<>();
		criteriaChain.add(this);
		key = N1QLExpression.WRAPPER();
		operator = "";
		value = new Object[] { qc };
		chainOperator = null;
	}

	// wrap criteria (including the criteriaChain) in a new QueryCriteria and set the queryChain of the original criteria
	// to just itself. The new query will be the value[0] of the original query.
	private void wrapThis() {
		QueryCriteria qc = new QueryCriteria(this.criteriaChain, this.key, this.value, this.chainOperator, this.operator,
				this.format);
		this.criteriaChain.remove(this); // we replaced it with the clone
		this.criteriaChain = new LinkedList<>();
		this.criteriaChain.add(this);
		this.key = N1QLExpression.WRAPPER();
		this.operator = "";
		this.format = null;
		this.value = new Object[] { qc };
		this.chainOperator = null;

	}

	// wrap criteria (including the criteriaChain) in a new QueryCriteria and set the queryChain of the original criteria
	// to just itself. The new query will be the value[0] of the original query.
	private static QueryCriteria wrap(QueryCriteria criteria) {
		QueryCriteria qc = new QueryCriteria(criteria.criteriaChain, criteria.key, criteria.value, criteria.chainOperator,
				criteria.operator, criteria.format);
		criteria.criteriaChain.remove(qc);
		int idx = criteria.criteriaChain.indexOf(criteria);
		criteria.criteriaChain.add(idx, qc);
		criteria.criteriaChain.remove(criteria); // we replaced it with the clone
		criteria.criteriaChain = new LinkedList<>();
		criteria.criteriaChain.add(criteria);
		criteria.key = N1QLExpression.WRAPPER();
		criteria.operator = "";
		criteria.format = null;
		criteria.value = new Object[] { qc };
		criteria.chainOperator = null;
		return criteria;
	}

	public QueryCriteria and(String key) {
		return and(x(key));
	}

	public QueryCriteria and(N1QLExpression key) {
		// this.criteriaChain.getLast().setChainOperator(ChainOperator.AND);
		return new QueryCriteria(this.criteriaChain, key, null, ChainOperator.AND);
	}

	public QueryCriteria and(QueryCriteria criteria) {
		if (this.criteriaChain != null && !this.criteriaChain.contains(this)) {
			throw new RuntimeException("criteria chain does not include this");
		}
		if (this.criteriaChain == null) {
			this.criteriaChain = new LinkedList<>();
			this.criteriaChain.add(this);
		}
		QueryCriteria newThis = wrap(this);
		QueryCriteria qc = wrap(criteria);
		newThis.criteriaChain.add(qc);
		qc.setChainOperator(ChainOperator.AND);
		newThis.chainOperator = ChainOperator.AND;//  otherwise we get "A chain operator must be present when chaining"
		return newThis;
	}

	// upper and lower should work like and/or by pushing "this" to a 'value' and changing "this" to upper()/lower()
	public QueryCriteria upper() {
		key = x("UPPER(" + key + ")");
		operator = "UPPER";
		value = new Object[] {};
		return this;
	}

	// upper and lower should work like and/or by pushing "this" to a 'value' and changing "this" to upper()/lower()
	public QueryCriteria lower() {
		key = x("LOWER(" + key + ")");
		operator = "LOWER";
		value = new Object[] {};
		return this;
	}

	public QueryCriteria or(String key) {
		return or(x(key));
	}

	public QueryCriteria or(N1QLExpression key) {
		// this.criteriaChain.getLast().setChainOperator(ChainOperator.OR);
		return new QueryCriteria(this.criteriaChain, key, null, ChainOperator.OR);
	}

	public QueryCriteria or(QueryCriteria criteria) {
		if (this.criteriaChain != null && !this.criteriaChain.contains(this)) {
			throw new RuntimeException("criteria chain does not include this");
		}
		if (this.criteriaChain == null) {
			this.criteriaChain = new LinkedList<>();
			this.criteriaChain.add(this);
		}
		QueryCriteria newThis = wrap(this);
		QueryCriteria qc = wrap(criteria);
		qc.criteriaChain = newThis.criteriaChain;
		newThis.criteriaChain.add(qc);
		qc.setChainOperator(ChainOperator.OR);
		newThis.chainOperator = ChainOperator.OR;//  otherwise we get "A chain operator must be present when chaining"
		return newThis;
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

	public QueryCriteria arrayContaining(@Nullable Object o) {
		operator = "ARRAY_CONTAINING";
		value = new Object[] { o };
		format = "array_containing(%1$s, %3$s)";
		return this;
	}

	public QueryCriteria notContaining(@Nullable Object o) {
		replaceThisAsWrapperOf(containing(o));
		operator = "NOT";
		format = "not( %3$s )";
		return this;
	}

	public QueryCriteria negate() {
		replaceThisAsWrapperOf(this);
		operator = "NOT";
		format = "not( %3$s )";
		// criteriaChain = new LinkedList<>();
		// criteriaChain.add(this);
		return this;
	}

	public QueryCriteria size() {
		replaceThisAsWrapperOf(this);
		operator = "LENGTH";
		format = "LENGTH( %3$s )";
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
		return this;
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
		return this;
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
		return this;
	}

	public QueryCriteria within(@Nullable Object o) {
		operator = "WITHIN";
		value = new Object[] { o };
		format = "%1$s within %3$s";
		return this;
	}

	public QueryCriteria between(@Nullable Object o1, @Nullable Object o2) {
		operator = "BETWEEN";
		value = new Object[] { o1, o2 };
		format = "%1$s between %3$s and %4$s";
		return this;
	}

	public QueryCriteria in(@Nullable Object... o) {
		operator = "IN";
		format = "%1$s in %3$s";
		value = new Object[1];
		if (o.length > 0) {
			if (o[0] instanceof JsonArray || o[0] instanceof List || o[0] instanceof Object[]) {
				if (o.length != 1) {
					throw new RuntimeException("IN cannot take multiple lists");
				}
				if (o[0] instanceof Object[]) {
					value[0] = o[0];
				} else if (o[0] instanceof JsonArray) {
					JsonArray ja = ((JsonArray) o[0]);
					value[0] = ja.toList().toArray();
				} else if (o[0] instanceof List) {
					List l = ((List) o[0]);
					value[0] = l.toArray();
				}
			} else {
				// N1qlQueryCreatorTests.queryParametersArray()
				// Query expected = (new Query()).addCriteria(where(i("firstname")).in("Oliver", "Charles"));
				if (o instanceof Object[]) {
					value[0] = o;
				} else {
					// see QueryCriteriaTests.testNestedNotIn() - if arg to notIn is not cast to Object
					// notIn((Object) new String[] { "Alabama", "Florida" }));
					throw new CouchbaseException("unhandled parameters "+o);
				}
			}

		}
		return this;
	}

	public QueryCriteria notIn(@Nullable Object... o) {
		return in(o).negate();
	}

	public QueryCriteria TRUE() { // true/false are reserved, use TRUE/FALSE
		value = null;
		operator = null;
		format = "%1$s"; // field = 1$, operator = 2$, value=$3, $4, ...
		return this;
	}

	public QueryCriteria FALSE() {
		value = null;
		operator = "NOT";
		format = "not(%1$s)"; // field = 1$, operator = 2$, value=$3, $4, ...
		return this;
	}

	/**
	 * This exports the query criteria chain into a string to be appended to the beginning of an N1QL statement
	 *
	 * @param paramIndexPtr - this is a reference to the parameter index to be used for positional parameters There may
	 *          already be positional parameters in the beginning of the statement, so it may not always start at 1. If it
	 *          has the value -1, the query is using named parameters. If the pointer is null, the query is not using
	 *          parameters.
	 * @param parameters - parameters of the query. If operands are parameterized, their values are added to parameters
	 * @return string containing part of N1QL query
	 */
	@Override
	public String export(int[] paramIndexPtr, JsonValue parameters, CouchbaseConverter converter) {
		StringBuilder output = new StringBuilder();
		boolean first = true;
		for (QueryCriteria c : this.criteriaChain) {
			if (!first) {
				if (c.chainOperator == null) {
					throw new IllegalStateException("A chain operator must be present when chaining! \n" + c);
				}
				// the consistent place to output this would be in the c.exportSingle(output) about five lines down
				output.append(" ").append(c.chainOperator.representation).append(" ");
			} else {
				first = false;
			}
			c.exportSingle(output, paramIndexPtr, parameters, converter);
		}

		return output.toString();
	}

	/**
	 * Export the query criteria to a string without using positional or named parameters.
	 *
	 * @return string containing part of N1QL query
	 */
	@Override
	public String export() { // used only by tests
		return export(null, null, null);

	}

	/**
	 * Appends the query criteria to a StringBuilder which will be appended to a N1QL statement
	 *
	 * @param sb - the string builder
	 * @param paramIndexPtr - this is a reference to the parameter index to be used for positional parameters There may
	 *          already be positional parameters in the beginning of the statement, so it may not always start at 1. If it
	 *          has the value -1, the query is using named parameters. If the pointer is null, the query is not using
	 *          parameters.
	 * @param parameters - parameters of the query. If operands are parameterized, their values are added to parameters
	 * @return string containing part of N1QL query
	 */
	private StringBuilder exportSingle(StringBuilder sb, int[] paramIndexPtr, JsonValue parameters,
			CouchbaseConverter converter) {
		String fieldName = key == null ? null : key.toString(); // maybeBackTic(key);
		int valueLen = value == null ? 0 : value.length;
		Object[] v = new Object[valueLen + 2];
		v[0] = fieldName;
		v[1] = operator;
		for (int i = 0; i < valueLen; i++) {
			if (value[i] instanceof QueryCriteria) {
				v[i + 2] = "(" + ((QueryCriteria) value[i]).export(paramIndexPtr, parameters, converter) + ")";
			} else {
				v[i + 2] = maybeWrapValue(key, value[i], paramIndexPtr, parameters, converter);
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

	/**
	 * Possibly convert an operand to a positional or named parameter
	 *
	 * @param paramIndexPtr - this is a reference to the parameter index to be used for positional parameters There may
	 *          already be positional parameters in the beginning of the statement, so it may not always start at 1. If it
	 *          has the value -1, the query is using named parameters. If the pointer is null, the query is not using
	 *          parameters.
	 * @param parameters - parameters of the query. If operands are parameterized, their values are added to parameters
	 * @return string containing part of N1QL query
	 */
	private String maybeWrapValue(N1QLExpression key, Object value, int[] paramIndexPtr, JsonValue parameters,
			CouchbaseConverter converter) {
		if (paramIndexPtr != null) {
			if (paramIndexPtr[0] >= 0) {
				JsonArray params = (JsonArray) parameters;
				// from StringBasedN1qlQueryParser.getPositionalPlaceholderValues()

				if (value instanceof Object[] || value instanceof Collection) {
					addAsCollection(params, asCollection(value), converter);
				} else {
					params.add(convert(converter, value));
				}

				return "$" + (++paramIndexPtr[0]); // these are generated in order
			} else {
				JsonObject params = (JsonObject) parameters;
				// from StringBasedN1qlQueryParser.getNamedPlaceholderValues()
				try {
					params.put(key.toString(), convert(converter, value));
				} catch (InvalidArgumentException iae) {
					if (value instanceof Object[]) {
						params.put(key.toString(), JsonArray.from((Object[]) value));
					} else {
						throw iae;
					}
				}
				return "$" + key;
			}
		}

		// Did not convert to a parameter. Add quotes or whatever it might need.

		if (value instanceof String) {
			return "\"" + value + "\"";
		} else if (value == null) {
			return "null";
		} else if (value instanceof Object[]) { // convert array into sequence of comma-separated values
			StringBuilder l = new StringBuilder();
			l.append("[");
			Object[] array = (Object[]) value;
			for (int i = 0; i < array.length; i++) {
				if (i > 0) {
					l.append(",");
				}
				l.append(maybeWrapValue(null, array[i], null, null, converter));
			}
			l.append("]");
			return l.toString();
		} else {
			return value.toString();
		}
	}

	private static Object convert(CouchbaseConverter converter, Object value) {
		return converter != null ? converter.convertForWriteIfNeeded(value) : value;
	}

	private void addAsCollection(JsonArray posValues, Collection collection, CouchbaseConverter converter) {
		JsonArray ja = JsonValue.ja();
		for (Object e : collection) {
			ja.add(String.valueOf(convert(converter, e)));
		}
		posValues.add(ja);
	}

	/**
	 * Returns a collection from the given source object. From MappingCouchbaseConverter.
	 *
	 * @param source the source object.
	 * @return the target collection.
	 */
	private static Collection<?> asCollection(final Object source) {
		if (source instanceof Collection) {
			return (Collection<?>) source;
		}
		return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
	}

	private String maybeBackTic(String value) {
		if (value == null || (value.startsWith("`") && value.endsWith("`"))) {
			return value;
		} else {
			return "`" + value + "`";
		}
	}

	@Override
	public void setChainOperator(ChainOperator chainOperator) {
		this.chainOperator = chainOperator;
	}

	public enum ChainOperator {
		AND("and"), OR("or");

		private final String representation;

		ChainOperator(String representation) {
			this.representation = representation;
		}

		public static ChainOperator from(String operator) {
			return valueOf(operator.substring(1).toUpperCase());
		}

		public String getRepresentation() {
			return representation;
		}
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("{key: " + this.key);
		sb.append(", operator: " + this.operator);
		sb.append(", value: " + this.value);
		sb.append(", criteriaChain.size(): " + this.criteriaChain.size() + "\n");
		for (QueryCriteria qc : criteriaChain) {
			sb.append("  key: " + qc.key + " operator: " + qc.operator + "\n");
		}
		sb.append("}");
		return sb.toString();
	}
}
