package org.springframework.data.couchbase.core.query;

import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.List;

public class QueryCriteria implements QueryCriteriaDefinition {

	/**
	 * Holds the chain itself, the current operator being always the last one.
	 */
	private final List<QueryCriteria> criteriaChain;

	/**
	 * Represents how the chain is hung together, null only for the first element.
	 */
	private ChainOperator chainOperator;

	private final String key;
	private String operator;
	private Object value;

	QueryCriteria(List<QueryCriteria> chain, String key, Object value, ChainOperator chainOperator) {
		this.criteriaChain = chain;
		criteriaChain.add(this);
		this.key = key;
		this.value = value;
		this.chainOperator = chainOperator;
	}

	/**
	 * Static factory method to create a Criteria using the provided key.
	 */
	public static QueryCriteria where(String key) {
		return new QueryCriteria(new ArrayList<>(), key, null, null);
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
		value = o;
		return this;
	}

	public QueryCriteria ne(@Nullable Object o) {
		operator = "!=";
		value = o;
		return this;
	}

	public QueryCriteria lt(@Nullable Object o) {
		operator = "<";
		value = o;
		return this;
	}

	public QueryCriteria lte(@Nullable Object o) {
		operator = "<=";
		value = o;
		return this;
	}

	public QueryCriteria gt(@Nullable Object o) {
		operator = ">";
		value = o;
		return this;
	}

	public QueryCriteria gte(@Nullable Object o) {
		operator = ">=";
		value = o;
		return this;
	}

	@Override
	public String export() {
		StringBuilder output = new StringBuilder();

		boolean first = true;
		for (QueryCriteria c : this.criteriaChain) {
			if (!first) {
				if (c.chainOperator == null) {
					throw new IllegalStateException("A chain operator must be present when chaining!");
				}

				output.append(" ").append(c.chainOperator.representation).append(" ");
			}
			if (first) {
				first = false;
			}
			c.exportSingle(output);
		}

		return output.toString();
	}

	protected void exportSingle(final StringBuilder sb) {
		if (value instanceof QueryCriteria) {
			sb.append("(").append(((QueryCriteria) value).export()).append(")");
		} else {
			String fieldName = "`" + key + "`";
			sb.append(fieldName).append(" ").append(operator).append(" ").append(maybeWrapValue(value));
		}
	}

	private String maybeWrapValue(Object value) {
		if (value instanceof String) {
			return "\"" + value + "\"";
		} else if (value == null) {
			return "null";
		} else {
			return value.toString();
		}
	}

	enum ChainOperator {
		AND("and"),
		OR("or"),
		NOT("not");

		private final String representation;

		ChainOperator(String representation) {
			this.representation = representation;
		}

		public String getRepresentation() {
			return representation;
		}
	}

}

