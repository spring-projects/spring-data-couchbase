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
