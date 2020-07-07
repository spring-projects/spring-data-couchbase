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

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonValue;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.repository.query.StringBasedN1qlQueryParser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Michael Reiche Query created from the string in @Query annotation in the repository
 *         interface. @Query("#{#n1ql.selectEntity} where firstname = $1 and lastname = $2") List<User>
 *         getByFirstnameAndLastname(String firstname, String lastname); It must include the SELECT ... FROM ...
 *         preferably via the #n1ql expression In addition to any predicates in the string, a predicate for the
 *         domainType (class) will be added.
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
	public String toN1qlString(ReactiveCouchbaseTemplate template, Class domainClass, boolean isCount) {
		StringBasedN1qlQueryParser.N1qlSpelValues n1ql = getN1qlSpelValues(template, domainClass, isCount);
		StringBuilder statement = new StringBuilder();
		appendInlineN1qlStatement(statement); // apply the string statement
		statement = insertWherePredOrAndPred(statement, n1ql.filter, n1ql.bucket); // typeKey = typeValue

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
		appendWhere(statement, paramIndexPtr); // criteria on this Query - should be empty for StringQuery
		appendSort(statement);
		appendSkipAndLimit(statement);
		return statement.toString();
	}

	/**
	 * Inserts or appends a predicate to a n1ql string 1) If the n1ql string already contains a WHERE, the predicate will
	 * be inserted right after the WHERE 2) If the n1ql string doesn't contain a WHERE, either a) if FROM 'bucket' is at
	 * the end of the n1ql statement, append WHERE <predicate> b) if FROM 'bucket' is not at the of the n1ql statement,
	 * insert WHERE <predicate> immediately after FROM 'bucket' </predicate>
	 * 
	 * @param sb - StringBuilder containing n1ql statement being built
	 * @param predicate - predicate to add to statement
	 * @param bucket - to find 'from bucket' in n1ql statement
	 * @return - StringBuilder containing n1ql statement being built
	 */
	private StringBuilder insertWherePredOrAndPred(StringBuilder sb, String predicate, String bucket) {
		String querySoFar = sb.toString().toUpperCase();
		Matcher whereMatcher = WHERE_PATTERN.matcher(querySoFar);
		boolean alreadyWhere = false;
		while (!alreadyWhere && whereMatcher.find()) {
			if (notQuoted(whereMatcher.start(), whereMatcher.end(), querySoFar)) {
				alreadyWhere = true;
			}
		}
		if (alreadyWhere) { // already a 'where', the predicate can go immediately after the 'where'
			StringBuilder newStringBuilder = new StringBuilder(sb.substring(0, whereMatcher.end()));
			newStringBuilder.append(predicate);
			newStringBuilder.append(" AND ");
			newStringBuilder.append(sb.substring(whereMatcher.end()));
			return newStringBuilder;
		} else { // if there isn't a 'where' (select * from bucket LIMIT 10), we need to insert following 'bucket'
			final Pattern BUCKET_PATTERN = Pattern.compile("\\s(f|F)(r|R)(o|O)(m|M)(\\s)+" + bucket + "(\\s|$)");
			Matcher fromBucketMatcher = BUCKET_PATTERN.matcher(sb.toString()); // do not change case
			boolean alreadyFromBucket = false;
			while (!alreadyFromBucket && fromBucketMatcher.find()) {
				if (notQuoted(fromBucketMatcher.start(), fromBucketMatcher.end(), querySoFar)) {
					alreadyFromBucket = true;
				}
			}
			if (!alreadyFromBucket) {
				throw new RuntimeException("bucket name " + bucket + " not found in query string: " + sb.toString());
			}
			if (fromBucketMatcher.end() == sb.toString().length()) { // at the end, just append
				sb.append(" WHERE ");
				sb.append(predicate);
				return sb;
			} else {
				StringBuilder newStringBuilder = new StringBuilder(sb.substring(0, fromBucketMatcher.end()));
				newStringBuilder.append(" WHERE ");
				newStringBuilder.append(predicate);
				newStringBuilder.append(" ");
				newStringBuilder.append(sb.substring(fromBucketMatcher.end()));
				return newStringBuilder;
			}
		}
	}
}
