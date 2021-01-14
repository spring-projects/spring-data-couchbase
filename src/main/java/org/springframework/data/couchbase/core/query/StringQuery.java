/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core.query;

import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonValue;

/**
 * Query created from the string in @Query annotation in the repository interface.
 * 
 * <pre>
 * &#64;Query("#{#n1ql.selectEntity} where #{#n1ql.filter} and firstname = $1 and lastname = $2")
 * List<User> getByFirstnameAndLastname(String firstname, String lastname);
 * </pre>
 * 
 * It must include the SELECT ... FROM ... preferably via the #n1ql expression, in addition to any predicates required,
 * including the n1ql.filter (for _class = className)
 * 
 * @author Michael Reiche
 */
public class StringQuery extends Query {

	private String inlineN1qlQuery;

	public StringQuery(String n1qlString) {
		inlineN1qlQuery = n1qlString;
	}

	/**
	 * inlineN1qlQuery (Query Annotation) append the string query to the provided StringBuilder. To be used along with the
	 * other append*() methods to construct the N1QL statement
	 * 
	 * @param sb - StringBuilder
	 */
	private void appendInlineN1qlStatement(final StringBuilder sb) {
		sb.append(inlineN1qlQuery);
	}

	@Override
	public String toN1qlSelectString(ReactiveCouchbaseTemplate template, String collection, Class domainClass,
			Class resultClass, boolean isCount, String[] distinctFields) {
		final StringBuilder statement = new StringBuilder();
		appendInlineN1qlStatement(statement); // apply the string statement
		// To use generated parameters for literals
		// we need to figure out if we must use positional or named parameters
		// If we are using positional parameters, we need to start where
		// the inlineN1ql left off.
		int[] paramIndexPtr = null;
		JsonValue params = this.getParameters();
		if (params instanceof JsonArray) { // positional parameters
			paramIndexPtr = new int[] { ((JsonArray) params).size() };
		} else { // named parameters or no parameters, no index required
			paramIndexPtr = new int[] { -1 };
		}
		appendWhere(statement, paramIndexPtr, template.getConverter()); // criteria on this Query - should be empty for
																																		// StringQuery
		appendSort(statement);
		appendSkipAndLimit(statement);
		return statement.toString();
	}
}
