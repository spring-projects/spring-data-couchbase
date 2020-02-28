package org.springframework.data.couchbase.core.query;

import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class QueryCriteria implements QueryCriteriaDefinition {

	private @Nullable String key;
	private List<QueryCriteria> criteriaChain;
	private LinkedHashMap<String, Object> criteria = new LinkedHashMap<>();
	private @Nullable ChainOperator chainOperator;
	private boolean nested;

	public QueryCriteria() {
		this.criteriaChain = new ArrayList<>();
	}

	public QueryCriteria(String key) {
		this.criteriaChain = new ArrayList<>();
		this.criteriaChain.add(this);
		this.key = key;
	}

	protected QueryCriteria(List<QueryCriteria> criteriaChain, String key, ChainOperator chainOperator, boolean nested) {
		this.criteriaChain = criteriaChain;
		this.criteriaChain.add(this);
		this.key = key;
		this.chainOperator = chainOperator;
		this.nested = nested;
	}

	/**
	 * Static factory method to create a Criteria using the provided key.
	 */
	public static QueryCriteria where(String key) {
		return new QueryCriteria(key);
	}

	public QueryCriteria and(String key) {
		return new QueryCriteria(this.criteriaChain, key, ChainOperator.AND, false);
	}

	public QueryCriteria or(String key) {
		return new QueryCriteria(this.criteriaChain, key, ChainOperator.OR, false);
	}

	public QueryCriteria eq(@Nullable Object o) {
		return is(o);
	}

	public QueryCriteria is(@Nullable Object o) {
		criteria.put("=", o);
		return this;
	}

	public QueryCriteria ne(@Nullable Object o) {
		criteria.put("!=", o);
		return this;
	}

	public QueryCriteria lt(@Nullable Object o) {
		criteria.put("<", o);
		return this;
	}

	public QueryCriteria lte(@Nullable Object o) {
		criteria.put("<=", o);
		return this;
	}

	public QueryCriteria gt(@Nullable Object o) {
		criteria.put(">", o);
		return this;
	}

	public QueryCriteria gte(@Nullable Object o) {
		criteria.put(">=", o);
		return this;
	}

	@Override
	public String export() {
		StringBuilder output = new StringBuilder();

		if (criteriaChain.size() == 1) {
			criteriaChain.get(0).exportSingle(output);
		} else if (CollectionUtils.isEmpty(this.criteriaChain) && !CollectionUtils.isEmpty(this.criteria)) {
			exportSingle(output);
		} else {
			boolean first = true;
			for (QueryCriteria c : this.criteriaChain) {
				boolean opened = false;

				if (!first) {
					if (c.chainOperator == null) {
						throw new IllegalStateException("A chain operator must be present when chaining!");
					}

					opened = true;
					output.append(" ").append(c.chainOperator.representation).append(" (");
				}
				if (first) {
					first = false;
				}
				c.exportSingle(output);

				if (opened) {
					output.append(")");
				}
			}
		}

		return output.toString();
	}

	protected void exportSingle(StringBuilder sb) {
		String fieldName = "`" + key + "`";

		for (Map.Entry<String, Object> entry : criteria.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();

			sb.append(fieldName).append(" ").append(key).append(" ").append(maybeWrapValue(value));
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

	@Override
	public String getKey() {
		return key;
	}

	enum ChainOperator {
		AND("and"),
		OR("or");

		private final String representation;

		ChainOperator(String representation) {
			this.representation = representation;
		}

		public String getRepresentation() {
			return representation;
		}
	}

}

