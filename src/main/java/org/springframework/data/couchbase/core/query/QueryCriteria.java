package org.springframework.data.couchbase.core.query;

import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;

public class QueryCriteria implements QueryCriteriaDefinition {

	/**
	 * Holds the chain itself, the current operator being always the last one.
	 */
	private  List<QueryCriteria> criteriaChain;

	/**
	 * Represents how the chain is hung together, null only for the first element.
	 */
	private ChainOperator chainOperator;

	private final String key;
	private String operator;
	private Object[] value;
	private String format;

	QueryCriteria(List<QueryCriteria> chain, String key, Object[] value, ChainOperator chainOperator) {
		this(chain, key, value, chainOperator, null, null);
	}

	QueryCriteria(List<QueryCriteria> chain, String key, Object value, ChainOperator chainOperator) {
		this(chain, key, new Object[]{value}, chainOperator, null, null);
	}

	QueryCriteria(List<QueryCriteria> chain, String key, Object[] value, ChainOperator chainOperator, String operator, String format) {
		this.criteriaChain = chain;
		if(this.criteriaChain != null)
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

	private static QueryCriteria wrap(QueryCriteria criteria) {
		QueryCriteria qc= new QueryCriteria(
				new LinkedList<QueryCriteria>() /*null */,
				criteria.key,
				criteria.value,
				null,
				criteria.operator,
				criteria.format);
		return qc;
	}

	public QueryCriteria eq(@Nullable Object o) {
		return is(o);
	}

	public QueryCriteria is(@Nullable Object o) {
		operator = "=";
		value = new Object[]{o};
		return this;
	}

	public QueryCriteria ne(@Nullable Object o) {
		operator = "!=";
		value = new Object[]{o};
		return this;
	}

	public QueryCriteria lt(@Nullable Object o) {
		operator = "<";
		value = new Object[]{o};
		return this;
	}

	public QueryCriteria lte(@Nullable Object o) {
		operator = "<=";
		value = new Object[]{o};
		return this;
	}

	public QueryCriteria gt(@Nullable Object o) {
		operator = ">";
		value = new Object[]{o};
		return this;
	}

	public QueryCriteria gte(@Nullable Object o) {
		operator = ">=";
		value = new Object[]{o};
		return this;
	}

	public QueryCriteria startingWith(@Nullable Object o) {
		regex("^"+o.toString());
		return this;
	}

	public QueryCriteria endingWith(@Nullable Object o) {
		regex(o.toString()+"$");
		return this;
	}

	public QueryCriteria regex(@Nullable Object o) {
		operator = "REGEX_CONTAINS";
		value = new Object[]{o};
		format = "%2$s (%1$s,   %3$s)";
		return this;
	}

	public QueryCriteria containing(@Nullable Object o) {
		operator = "CONTAINS";
		value = new Object[]{o};
		format = "%2$s (%1$s,   %3$s)";
		return this;
	}

	public QueryCriteria notcontaining(@Nullable Object o) {
		value=new QueryCriteria[]{wrap(containing(o))};
		operator ="NOT";
		format = format = "%2$s( %3$s )";
		chainOperator = ChainOperator.NOT;
		return (QueryCriteria)this;
	}

	public QueryCriteria like(@Nullable Object o) {
		operator = "REGEX_CONTAINS";
		value=new Object[]{o};// might want to transform from regex to 'like' (i.e. .* -> %, . -> ? )
		format = "%2$s (%1$s,   %3$s)";
		return this;
	}

	public QueryCriteria notlike(@Nullable Object o) {
		value=new QueryCriteria[]{wrap(like(o))};
		operator ="NOT";
		format = format = "%2$s( %3$s )";
		chainOperator = ChainOperator.NOT;
		return (QueryCriteria)this;
	}

	public QueryCriteria isnull(@Nullable Object... o) {
		operator = "IS_NULL";
		value=o;
		format = "%1$s is null";
		return this;
	}

	public QueryCriteria isnotnull(@Nullable Object... o) {
		operator ="IS_NOT_NULL";
		value=o;
		format = "%1$s is not null";
		chainOperator = ChainOperator.NOT;
		return (QueryCriteria)this;
	}

	public QueryCriteria ismissing(@Nullable Object... o) {
		operator = "IS_MISSING";
		value=o;
		format = "%1$s is missing";
		return this;
	}

	public QueryCriteria isnotmissing(@Nullable Object... o) {
		operator ="IS_NOT_MiSSING";
		value=o;
		format = "%1$s is not missing";
		chainOperator = ChainOperator.NOT;
		return (QueryCriteria)this;
	}

	public QueryCriteria isvalued(@Nullable Object... o) {
		operator = "IS_VALUED";
		value=o;
		format = "%1$s is valued";
		return this;
	}

	public QueryCriteria isnotvalued(@Nullable Object... o) {
		operator ="IS_NOT_VALUED";
		value=o;
		format = "%1$s is not valued";
		chainOperator = ChainOperator.NOT;
		return (QueryCriteria)this;
	}

	public QueryCriteria between(@Nullable Object o1, @Nullable Object o2) {
		operator ="BETWEEN";
		value=new Object[]{o1,o2};
		format = "%1$s BETWEEN %3$s AND %4$s";
		return (QueryCriteria)this;
	}

	public QueryCriteria in(@Nullable Object... o) {
		operator ="IN";
		value=o;
		StringBuilder sb = new StringBuilder ("%1$s IN ( [ ");
		for(int i=1; i<=value.length; i++) { // format indices start at 1
			if(i>1) sb.append(", ");
			sb.append("%" + (i+2) + "$s");  // the first is fieldName, second is operator, args start at 3
		}
		format=sb.append(" ] )").toString();
		return (QueryCriteria)this;
	}

	public QueryCriteria notin(@Nullable Object... o) {
		value=new QueryCriteria[]{wrap(in(o))};
		operator ="NOT";
		format = format = "%2$s( %3$s )"; // field = 1$, operator = 2$, value=$3, $4, ...
		chainOperator = ChainOperator.NOT;
		return (QueryCriteria)this;
	}

	public QueryCriteria TRUE(@Nullable Object... o) { // true/false are reserved, use TRUE/FALSE
		value=null;
		operator = null;
		format = format = "%1$s"; // field = 1$, operator = 2$, value=$3, $4, ...
		return (QueryCriteria)this;
	}

	public QueryCriteria FALSE(@Nullable Object... o) {
		value=new QueryCriteria[]{wrap(TRUE(o))};
		operator ="NOT";
		format = format = "%2$s( %3$s )";
		chainOperator = ChainOperator.NOT;
		return (QueryCriteria)this;
	}

	@Override
	public String export() {
		StringBuilder output = new StringBuilder();

		//if(criteriaChain != null ) {
			boolean first = true;
			for (QueryCriteria c : this.criteriaChain) {
				if (!first) {
					if (c.chainOperator == null) {
						throw new IllegalStateException("A chain operator must be present when chaining!");
					}
					// the consistent place to output this would be in the c.toQueryString(output) about five lines down
					output.append(" ").append(c.chainOperator.representation).append(" ");
				}
				if (first) {
					first = false;
				}
				c.toQueryString(output);
			}
		//} else {
		//	// shouldn't this be a chain of a single QueryCriteria?
		//	toQueryString(output);
		//}

		return output.toString();
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

	private StringBuilder toQueryString(StringBuilder sb){
		//StringBuilder sb=new StringBuilder();
		String fieldName="`"+key+"`";
		int valueLen = value == null ? 0 : value.length;
		Object[] v=new Object[valueLen+2];
		v[0]=fieldName;
		v[1]=operator;
		for(int i=0; i< valueLen; i++){
			if(value[i] instanceof QueryCriteria)
				v[i+2]="("+((QueryCriteria)value[i]).export()+")";
			else
				v[i+2]=maybeWrapValue(value[i]);
		}

		if(key==null) { // chaining, the chainingOperator was already output by export()
			sb.append(v[2]);
		} else if(format == null) // this always has to be fieldname <op> <something>
			sb.append(fieldName).append(" ").append(operator).append(" ").append(v[2]);
		else
			sb.append(new Formatter().format(format, v));

		return sb;
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

