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

import java.io.Serializable;
import java.util.Iterator;

/**
 * A N1QL Query Expression
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class N1QLExpression {
	private static final N1QLExpression NULL_INSTANCE = new N1QLExpression("NULL");
	private static final N1QLExpression TRUE_INSTANCE = new N1QLExpression("TRUE");
	private static final N1QLExpression FALSE_INSTANCE = new N1QLExpression("FALSE");
	private static final N1QLExpression MISSING_INSTANCE = new N1QLExpression("MISSING");
	private static final N1QLExpression EMPTY_INSTANCE = new N1QLExpression("");

	private Object value;

	private N1QLExpression(final Object value) {
		this.value = value;
	}

	/**
	 * Creates an arbitrary expression from the given string value. No quoting or escaping will be done on the input. In
	 * addition, it is not checked if the given value is an actual valid (N1QL syntax wise) expression.
	 *
	 * @param value the value to create the expression from.
	 * @return a new {@link N1QLExpression} representing the value.
	 */
	public static N1QLExpression x(final String value) {
		return new N1QLExpression(value);
	}

	/**
	 * An identifier or list of identifiers escaped using backquotes `. Useful for example for identifiers that contains a
	 * dash like "beer-sample". Multiple identifiers are returned as a list of escaped identifiers separated by ", ".
	 *
	 * @param identifiers the identifier(s) to escape.
	 * @return an {@link N1QLExpression} representing the escaped identifier.
	 */
	public static N1QLExpression i(final String... identifiers) {
		return new N1QLExpression(wrapWith('`', identifiers));
	}

	/**
	 * An identifier or list of identifiers which will be quoted as strings (with "").
	 *
	 * @param strings the list of strings to quote.
	 * @return an {@link N1QLExpression} representing the quoted strings.
	 */
	public static N1QLExpression s(final String... strings) {
		return new N1QLExpression(wrapWith('"', strings));
	}

	/**
	 * Returns an expression representing boolean TRUE.
	 *
	 * @return an expression representing TRUE.
	 */
	public static N1QLExpression TRUE() {
		return TRUE_INSTANCE;
	}

	/**
	 * Returns an expression representing boolean FALSE.
	 *
	 * @return an expression representing FALSE.
	 */
	public static N1QLExpression FALSE() {
		return FALSE_INSTANCE;
	}

	/**
	 * Returns an expression representing NULL.
	 *
	 * @return an expression representing NULL.
	 */
	public static N1QLExpression NULL() {
		return NULL_INSTANCE;
	}

	/**
	 * Returns an expression representing WRAPPER.
	 *
	 * @return an expression representing WRAPPER.
	 */
	public static N1QLExpression WRAPPER() {
		return EMPTY_INSTANCE;
	}

	/**
	 * Returns an expression representing MISSING.
	 *
	 * @return an expression representing MISSING.
	 */
	public static N1QLExpression MISSING() {
		return MISSING_INSTANCE;
	}

	/**
	 * Helper method to prefix a string.
	 *
	 * @param prefix the prefix.
	 * @param right the right side of the expression.
	 * @return a prefixed expression.
	 */
	private static N1QLExpression prefix(String prefix, String right) {
		return new N1QLExpression(prefix + " " + right);
	}

	/**
	 * Helper method to infix a string.
	 *
	 * @param infix the infix.
	 * @param left the left side of the expression.
	 * @param right the right side of the expression.
	 * @return a infixed expression.
	 */
	private static N1QLExpression infix(String infix, String left, String right) {
		return new N1QLExpression(left + " " + infix + " " + right);
	}

	/**
	 * Helper method to postfix a string.
	 *
	 * @param postfix the postfix.
	 * @param left the left side of the expression.
	 * @return a prefixed expression.
	 */
	private static N1QLExpression postfix(String postfix, String left) {
		return new N1QLExpression(left + " " + postfix);
	}

	/**
	 * Construct a path ("a.b.c") from Expressions or values. Strings are considered identifiers (so they won't be
	 * quoted).
	 *
	 * @param pathComponents the elements of the path, joined together by a dot.
	 * @return the path created from the given components.
	 */
	public static N1QLExpression path(Object... pathComponents) {
		if (pathComponents == null || pathComponents.length == 0) {
			return EMPTY_INSTANCE;
		}
		StringBuilder path = new StringBuilder();
		for (Object p : pathComponents) {
			path.append('.');
			if (p instanceof N1QLExpression) {
				path.append(((N1QLExpression) p).toString());
			} else {
				path.append(String.valueOf(p));
			}
		}
		path.deleteCharAt(0);
		return x(path.toString());
	}

	/**
	 * @return metadata for the document expression
	 */
	public static N1QLExpression meta(N1QLExpression expression) {
		return x("META(" + expression.toString() + ")");
	}

	/**
	 * Prepends a SELECT to the given expression
	 */
	public static N1QLExpression select(N1QLExpression... expressions) {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");

		for (int i = 0; i < expressions.length; i++) {
			sb.append(expressions[i].toString());
			if (i < expressions.length - 1) {
				sb.append(", ");
			}
		}
		return x(sb.toString());
	}

	/**
	 * Begins a delete statement
	 */
	public static N1QLExpression delete() {
		return x("DELETE");
	}

	/**
	 * Returned expression results in count of all the non-NULL and non-MISSING values in the group.
	 */
	public static N1QLExpression count(N1QLExpression expression) {
		return x("COUNT(" + expression.toString() + ")");
	}

	/**
	 * Helper method to wrap varargs with the given character.
	 *
	 * @param wrapper the wrapper character.
	 * @param input the input fields to wrap.
	 * @return a concatenated string with characters wrapped.
	 */
	private static String wrapWith(char wrapper, String... input) {
		StringBuilder escaped = new StringBuilder();
		for (String i : input) {
			escaped.append(", ");
			escaped.append(wrapper).append(i).append(wrapper);
		}
		if (escaped.length() > 2) {
			escaped.delete(0, 2);
		}
		return escaped.toString();
	}

	/**
	 * AND-combines two expressions.
	 *
	 * @param right the expression to combine with the current one.
	 * @return a combined expression.
	 */
	public N1QLExpression and(N1QLExpression right) {
		return infix("AND", toString(), right.toString());
	}

	/**
	 * OR-combines two expressions.
	 *
	 * @param right the expression to combine with the current one.
	 * @return a combined expression.
	 */
	public N1QLExpression or(N1QLExpression right) {
		return infix("OR", toString(), right.toString());
	}

	/**
	 * Adds a AS clause between the current and the given expression. Often used to alias an identifier.
	 *
	 * @param alias the right hand side expression.
	 * @return a new expression with the clause applied.
	 */
	public N1QLExpression as(N1QLExpression alias) {
		return infix("AS", toString(), alias.toString());
	}

	public N1QLExpression from(N1QLExpression bucketName) {
		return infix("FROM", toString(), bucketName.toString());
	}

	public N1QLExpression from(String bucketName) {
		return from(i(bucketName));
	}

	public N1QLExpression where(N1QLExpression right) {
		return infix("WHERE", toString(), right.toString());
	}

	public N1QLExpression returning(N1QLExpression right) {
		return infix("RETURNING", toString(), right.toString());
	}

	public N1QLExpression keys(Iterable<? extends Serializable> ids) {
		StringBuilder sb = new StringBuilder();
		Iterator<?> it = ids.iterator();
		sb.append("[");
		while (it.hasNext()) {
			sb.append(s(it.next().toString()));
			if (it.hasNext()) {
				sb.append(",");
			}
		}
		sb.append("]");
		return infix("USE KEYS", toString(), sb.toString());
	}

	/**
	 * Returned expression results in the given expression in lowercase.
	 */
	public N1QLExpression lower() {
		return x("LOWER(" + toString() + ")");
	}

	/**
	 * Returned expression will be converted to a string
	 */

	public N1QLExpression convertToString() {
		return x("TOSTRING(" + toString() + ")");
	}

	/**
	 * Combines two expressions with the equals operator ("=").
	 *
	 * @param right the expression to combine.
	 * @return the combined expressions.
	 */
	public N1QLExpression eq(N1QLExpression right) {
		return infix("=", toString(), right.toString());
	}

	public N1QLExpression eq(final boolean value) {
		return value ? eq(TRUE_INSTANCE) : eq(FALSE_INSTANCE);
	}

	public N1QLExpression asc() {
		return postfix("ASC", toString());
	}

	public N1QLExpression desc() {
		return postfix("DESC", toString());
	}

	public N1QLExpression limit(int limit) {
		return infix("LIMIT", toString(), String.valueOf(limit));
	}

	public N1QLExpression offset(int offset) {
		return infix("OFFSET", toString(), String.valueOf(offset));
	}

	/**
	 * Adds a BETWEEN clause between the current and the given expression.
	 *
	 * @param right the right hand side expression.
	 * @return a new expression with the clause applied.
	 */
	public N1QLExpression between(N1QLExpression right) {
		return infix("BETWEEN", toString(), right.toString());
	}

	/**
	 * Combines two expressions with the greater than operator ("&gt;").
	 *
	 * @param right the expression to combine.
	 * @return the combined expressions.
	 */
	public N1QLExpression gt(N1QLExpression right) {
		return infix(">", toString(), right.toString());
	}

	/**
	 * Appends a "IS NULL" to the expression.
	 *
	 * @return the postfixed expression.
	 */
	public N1QLExpression isNull() {
		return postfix("IS NULL", toString());
	}

	/**
	 * Appends a "IS NOT NULL" to the expression.
	 *
	 * @return the postfixed expression.
	 */
	public N1QLExpression isNotNull() {
		return postfix("IS NOT NULL", toString());
	}

	/**
	 * Combines two expressions with the not equals operator ("!=").
	 *
	 * @param right the expression to combine.
	 * @return the combined expressions.
	 */
	public N1QLExpression ne(N1QLExpression right) {
		return infix("!=", toString(), right.toString());
	}

	// ===== HELPERS =====

	/**
	 * Combines two expressions with the greater or equals than operator ("&gt;=").
	 *
	 * @param right the expression to combine.
	 * @return the combined expressions.
	 */
	public N1QLExpression gte(N1QLExpression right) {
		return infix(">=", toString(), right.toString());
	}

	/**
	 * Combines two expressions with the less or equals than operator ("&lt;=").
	 *
	 * @param right the expression to combine.
	 * @return the combined expressions.
	 */
	public N1QLExpression lte(N1QLExpression right) {
		return infix("<=", toString(), right.toString());
	}

	/**
	 * Adds a LIKE clause between the current and the given expression.
	 *
	 * @param right the right hand side expression.
	 * @return a new expression with the clause applied.
	 */
	public N1QLExpression like(N1QLExpression right) {
		return infix("LIKE", toString(), right.toString());
	}

	/**
	 * Adds a NOT LIKE clause between the current and the given expression.
	 *
	 * @param right the right hand side expression.
	 * @return a new expression with the clause applied.
	 */
	public N1QLExpression notLike(N1QLExpression right) {
		return infix("NOT LIKE", toString(), right.toString());
	}

	/**
	 * Adds a IN clause between the current and the given expression.
	 *
	 * @param right the right hand side expression.
	 * @return a new expression with the clause applied.
	 */
	public N1QLExpression in(N1QLExpression right) {
		return infix("IN", toString(), right.toString());
	}

	/**
	 * Adds a NOT IN clause between the current and the given expression.
	 *
	 * @param right the right hand side expression.
	 * @return a new expression with the clause applied.
	 */
	public N1QLExpression notIn(N1QLExpression right) {
		return infix("NOT IN", toString(), right.toString());
	}

	/**
	 * Appends a "IS NOT MISSING" to the expression.
	 *
	 * @return the postfixed expression.
	 */
	public N1QLExpression isNotMissing() {
		return postfix("IS NOT MISSING", toString());
	}

	/**
	 * Combines two expressions with the less than operator ("&lt;").
	 *
	 * @param right the expression to combine.
	 * @return the combined expressions.
	 */
	public N1QLExpression lt(N1QLExpression right) {
		return infix("<", toString(), right.toString());
	}

	public N1QLExpression orderBy(N1QLExpression... expressions) {
		// all the expressions just need to be separated by commas...
		StringBuilder sbOrderClause = new StringBuilder();
		for (int i = 0; i < expressions.length; i++) {
			sbOrderClause.append(expressions[i].toString());
			if (expressions.length - 1 > i) {
				sbOrderClause.append(", ");
			}
		}
		return infix("ORDER BY", toString(), sbOrderClause.toString());
	}

	@Override
	public String toString() {
		return value.toString();
	}
}
