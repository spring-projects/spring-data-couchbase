/*
 * Copyright 2017-2020 the original author or authors
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

package org.springframework.data.couchbase.repository.query.support;

import static org.springframework.data.couchbase.core.query.N1QLExpression.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.repository.query.ConvertingIterator;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.repository.query.parser.Part;

import com.couchbase.client.java.json.JsonArray;

/**
 * Utils for creating part tree expressions
 *
 * @author Subhashni Balakrishnan
 */
public class N1qlQueryCreatorUtils {
	public static N1QLExpression prepareExpression(CouchbaseConverter converter, Part part, Iterator<Object> iterator,
			AtomicInteger position, JsonArray placeHolderValues) {
		PersistentPropertyPath<CouchbasePersistentProperty> path = N1qlUtils.getPathWithAlternativeFieldNames(converter,
				part.getProperty());
		ConvertingIterator parameterValues = new ConvertingIterator(iterator, converter);

		// get the whole doted path with fieldNames instead of potentially wrong propNames
		String fieldNamePath = N1qlUtils.getDottedPathWithAlternativeFieldNames(path);

		// deal with ignore case
		boolean ignoreCase = false;
		Class<?> leafType = converter.getWriteClassFor(path.getLeafProperty().getType());
		boolean isString = leafType == String.class;
		if (part.shouldIgnoreCase() == Part.IgnoreCaseType.WHEN_POSSIBLE) {
			ignoreCase = isString;
		} else if (part.shouldIgnoreCase() == Part.IgnoreCaseType.ALWAYS) {
			if (!isString) {
				throw new IllegalArgumentException(
						String.format("Part %s must be of type String but was %s", fieldNamePath, leafType));
			}
			ignoreCase = true;
		}

		return createExpression(part.getType(), fieldNamePath, ignoreCase, parameterValues, position, placeHolderValues);
	}

	public static N1QLExpression createExpression(Part.Type partType, String fieldNamePath, boolean ignoreCase,
			Iterator<Object> parameterValues, AtomicInteger position, JsonArray placeHolderValues) {
		// create the left hand side of the expression, taking ignoreCase into account

		N1QLExpression left = ignoreCase ? (x(fieldNamePath).lower()) : x(fieldNamePath);
		N1QLExpression exp;
		switch (partType) {
			case BETWEEN:
				exp = left.between(getPlaceHolder(position, ignoreCase).and(getPlaceHolder(position, ignoreCase)));
				placeHolderValues.add(getValue(parameterValues));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case IS_NOT_NULL:
				exp = left.isNotNull();
				break;
			case IS_NULL:
				exp = left.isNull();
				break;
			case NEGATING_SIMPLE_PROPERTY:
				exp = left.ne(getPlaceHolder(position, ignoreCase));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case SIMPLE_PROPERTY:
				exp = left.eq(getPlaceHolder(position, ignoreCase));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case BEFORE:
			case LESS_THAN:
				exp = left.lt(getPlaceHolder(position, ignoreCase));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case LESS_THAN_EQUAL:
				exp = left.lte(getPlaceHolder(position, ignoreCase));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case GREATER_THAN_EQUAL:
				exp = left.gte(getPlaceHolder(position, ignoreCase));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case AFTER:
			case GREATER_THAN:
				exp = left.gt(getPlaceHolder(position, ignoreCase));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case NOT_LIKE:
				exp = left.notLike(getPlaceHolder(position, ignoreCase));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case LIKE:
				exp = left.like(getPlaceHolder(position, ignoreCase));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case STARTING_WITH:
				exp = left.like(x(getPlaceHolder(position, ignoreCase) + " || '%'"));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case ENDING_WITH:
				exp = left.like(x("'%' || " + getPlaceHolder(position, ignoreCase)));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case NOT_CONTAINING:
				exp = left.notLike(x("'%' || " + getPlaceHolder(position, ignoreCase) + " || '%'"));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case CONTAINING:
				exp = left.like(x("'%' || " + getPlaceHolder(position, ignoreCase) + " || '%'"));
				placeHolderValues.add(getValue(parameterValues));
				break;
			case NOT_IN:
				exp = left.notIn(getPlaceHolder(position, false));
				placeHolderValues.add(getArray(parameterValues));
				break;
			case IN:
				exp = left.in(getPlaceHolder(position, false));
				placeHolderValues.add(getArray(parameterValues));
				break;
			case TRUE:
				exp = left.eq(true);
				break;
			case FALSE:
				exp = left.eq(false);
				break;
			case REGEX:
				exp = x("REGEXP_LIKE(" + left.toString() + ", " + getPlaceHolder(position, false) + ")");
				placeHolderValues.add(getValueAsString(parameterValues));
				break;
			case EXISTS:
				exp = left.isNotMissing();
				break;
			case WITHIN:
			case NEAR:
			default:
				throw new IllegalArgumentException("Unsupported keyword in N1QL query derivation");
		}
		return exp;
	}

	protected static N1QLExpression getPlaceHolder(AtomicInteger position, boolean ignoreCase) {
		N1QLExpression placeHolder = x("$" + position.getAndIncrement());
		if (ignoreCase) {
			placeHolder = placeHolder.lower();
		}
		return placeHolder;
	}

	protected static String getValueAsString(Iterator<Object> parameterValues) {
		Object next = parameterValues.next();

		String pattern;
		if (next == null) {
			pattern = "";
		} else {
			pattern = String.valueOf(next);
		}
		return pattern;
	}

	protected static N1QLExpression like(Iterator<Object> parameterValues, boolean ignoreCase, boolean anyPrefix,
			boolean anySuffix) {
		Object next = parameterValues.next();
		if (next == null) {
			return N1QLExpression.NULL();
		}

		N1QLExpression converted;
		if (next instanceof String) {
			String pattern = (String) next;
			if (anyPrefix) {
				pattern = "%" + pattern;
			}
			if (anySuffix) {
				pattern = pattern + "%";
			}
			converted = s(pattern);

		} else {
			converted = x(String.valueOf(next));
		}

		if (ignoreCase) {
			return converted.lower();
		}
		return converted;
	}

	protected static Object getValue(Iterator<Object> parameterValues) {
		Object next = parameterValues.next();
		if (next instanceof Enum) {
			next = String.valueOf(next);
		}
		return next;
	}

	protected static JsonArray getArray(Iterator<Object> parameterValues) {
		Object next = parameterValues.next();

		Object[] values;
		if (next instanceof Collection) {
			values = ((Collection<?>) next).toArray();
		} else if (next.getClass().isArray()) {
			values = (Object[]) next;
		} else {
			values = new Object[] { next };
		}
		return JsonArray.from(values);
	}
}
