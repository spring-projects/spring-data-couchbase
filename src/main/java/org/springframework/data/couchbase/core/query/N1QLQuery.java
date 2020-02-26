package org.springframework.data.couchbase.core.query;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;

public class N1QLQuery {
	private N1QLExpression expression;
	private QueryOptions options;

	public N1QLQuery(N1QLExpression expression, QueryOptions options) {
		this.expression = expression;
		this.options = options;
	}

	public N1QLQuery(N1QLExpression expression) {
		this(expression, QueryOptions.queryOptions());
	}

	public String getExpression() {
		return expression.toString();
	}

	public QueryOptions getOptions() {
		return options;
	}

	public JsonObject n1ql() {
		JsonObject query = JsonObject.create().put("statement", expression.toString());
		options.build().injectParams(query);
		return query;
	}

}
