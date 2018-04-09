/*
 * Copyright 2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.query.support;

import static com.couchbase.client.java.query.dsl.Expression.*;

import java.util.Collection;
import java.util.Iterator;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.repository.query.ConvertingIterator;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.repository.query.parser.Part;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.functions.PatternMatchingFunctions;
import com.couchbase.client.java.query.dsl.functions.StringFunctions;

/**
 * Utils for creating part tree expressions
 *
 * @author Subhashni Balakrishnan
 */
public class N1qlQueryCreatorUtils {
    public static Expression prepareExpression(CouchbaseConverter converter, Part part, Iterator<Object> iterator) {
        PersistentPropertyPath<CouchbasePersistentProperty> path = N1qlUtils.getPathWithAlternativeFieldNames(
                converter, part.getProperty());
        ConvertingIterator parameterValues = new ConvertingIterator(iterator, converter);

        //get the whole doted path with fieldNames instead of potentially wrong propNames
        String fieldNamePath = N1qlUtils.getDottedPathWithAlternativeFieldNames(path);

        //deal with ignore case
        boolean ignoreCase = false;
        Class<?> leafType = converter.getWriteClassFor(path.getLeafProperty().getType());
        boolean isString = leafType == String.class;
        if (part.shouldIgnoreCase() == Part.IgnoreCaseType.WHEN_POSSIBLE) {
            ignoreCase = isString;
        } else if (part.shouldIgnoreCase() == Part.IgnoreCaseType.ALWAYS) {
            if (!isString) {
                throw new IllegalArgumentException(String.format("Part %s must be of type String but was %s", fieldNamePath, leafType));
            }
            ignoreCase = true;
        }

        return createExpression(part.getType(), fieldNamePath, ignoreCase, parameterValues);
    }


    public static Expression createExpression(Part.Type partType, String fieldNamePath, boolean ignoreCase, Iterator<Object> parameterValues) {
        //create the left hand side of the expression, taking ignoreCase into account
        Expression left = ignoreCase ? StringFunctions.lower(x(fieldNamePath)) : x(fieldNamePath);

        switch (partType) {
            case BETWEEN:
                return left.between(leftAndRight(parameterValues, ignoreCase));
            case IS_NOT_NULL:
                return left.isNotNull();
            case IS_NULL:
                return left.isNull();
            case NEGATING_SIMPLE_PROPERTY:
                return left.ne(right(parameterValues, ignoreCase));
            case SIMPLE_PROPERTY:
                return left.eq(right(parameterValues, ignoreCase));
            case BEFORE:
            case LESS_THAN:
                return left.lt(right(parameterValues, ignoreCase));
            case LESS_THAN_EQUAL:
                return left.lte(right(parameterValues, ignoreCase));
            case GREATER_THAN_EQUAL:
                return left.gte(right(parameterValues, ignoreCase));
            case AFTER:
            case GREATER_THAN:
                return left.gt(right(parameterValues, ignoreCase));
            case NOT_LIKE:
                return left.notLike(right(parameterValues, ignoreCase));
            case LIKE:
                return left.like(right(parameterValues, ignoreCase));
            case STARTING_WITH:
                return left.like(like(parameterValues, ignoreCase, false, true));
            case ENDING_WITH:
                return left.like(like(parameterValues, ignoreCase, true, false));
            case NOT_CONTAINING:
                return left.notLike(like(parameterValues, ignoreCase, true, true));
            case CONTAINING:
                return left.like(like(parameterValues, ignoreCase, true, true));
            case NOT_IN:
                return left.notIn(rightArray(parameterValues));
            case IN:
                return left.in(rightArray(parameterValues));
            case TRUE:
                return left.eq(true);
            case FALSE:
                return left.eq(false);
            case REGEX:
                return regexp(fieldNamePath, parameterValues);
            case EXISTS:
                return left.isNotMissing();
            case WITHIN:
            case NEAR:
            default:
                throw new IllegalArgumentException("Unsupported keyword in N1QL query derivation");
        }
    }


    protected static Expression regexp(String left, Iterator<Object> parameterValues) {
        Object next = parameterValues.next();

        String pattern;
        if (next == null) {
            pattern = "";
        } else {
            pattern = String.valueOf(next);
        }
        return PatternMatchingFunctions.regexpLike(left, pattern);
    }

    protected static Expression leftAndRight(Iterator<Object> parameterValues, boolean ignoreCase) {
        return right(parameterValues, ignoreCase).and(right(parameterValues, ignoreCase));
    }

    protected static Expression like(Iterator<Object> parameterValues, boolean ignoreCase,
                                     boolean anyPrefix, boolean anySuffix) {
        Object next = parameterValues.next();
        if (next == null) {
            return Expression.NULL();
        }

        Expression converted;
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
            return StringFunctions.lower(converted);
        }
        return converted;
    }

    protected static Expression right(Iterator<Object> parameterValues, boolean ignoreCase) {
        Object next = parameterValues.next();
        if (next == null) {
            return Expression.NULL();
        }

        Expression converted;
        if (next instanceof String) {
            converted = s((String) next);
        } else if (next instanceof Enum) {
            converted = s(String.valueOf(next));
        } else {
            converted = x(String.valueOf(next));
        }

        if (ignoreCase) {
            return StringFunctions.lower(converted);
        }
        return converted;
    }

    protected static JsonArray rightArray(Iterator<Object> parameterValues) {
        Object next = parameterValues.next();

        Object[] values;
        if (next instanceof Collection) {
            values = ((Collection<?>) next).toArray();
        } else if (next.getClass().isArray()) {
            values = (Object[]) next;
        } else {
            values = new Object[] {next};
        }
        return JsonArray.from(values);
    }
}
