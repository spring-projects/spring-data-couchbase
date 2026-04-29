/*
 * Copyright 2025-present the original author or authors
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
package org.springframework.data.couchbase.repository.query;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParametersParameterAccessor;

/**
 * Shared support for repository-backed FTS query string binding.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
final class SearchRepositoryQuerySupport {

	private static final Pattern POSITIONAL_PARAM_PATTERN = Pattern.compile("\\?(\\d+)");

	private SearchRepositoryQuerySupport() {
	}

	static String bindQueryString(String template, ParametersParameterAccessor accessor) {
		Matcher matcher = POSITIONAL_PARAM_PATTERN.matcher(template);
		if (!matcher.find()) {
			return template;
		}

		StringBuilder sb = new StringBuilder();
		matcher.reset();

		while (matcher.find()) {
			int index = Integer.parseInt(matcher.group(1));
			Object value = accessor.getBindableValue(index);
			matcher.appendReplacement(sb, Matcher.quoteReplacement(renderBindableValue(index, value)));
		}

		matcher.appendTail(sb);
		return sb.toString();
	}

	static void validateSort(ParametersParameterAccessor accessor) {
		Sort sort = accessor.getSort();
		if (sort.isSorted()) {
			throw new InvalidDataAccessApiUsageException(
					"Spring Sort/Pageable sorting is not supported for @Search repository methods. "
							+ "FTS SearchSort semantics are richer than Spring Sort; use the template API with explicit SearchSorts.");
		}
	}

	private static String renderBindableValue(int index, Object value) {
		if (value == null) {
			throw new InvalidDataAccessApiUsageException(
					"@Search parameter ?" + index + " resolved to null. Null values are not supported in FTS query strings.");
		}

		if (value instanceof Iterable<?> || value instanceof Map<?, ?> || value.getClass().isArray()) {
			throw new InvalidDataAccessApiUsageException(
					"@Search parameter ?" + index + " must be a scalar value. Collections, arrays, and maps are not supported; "
							+ "build a SearchRequest explicitly for complex FTS queries.");
		}

		if (value instanceof Number || value instanceof Boolean) {
			return value.toString();
		}

		return quoteString(value instanceof Enum<?> ? ((Enum<?>) value).name() : value.toString());
	}

	private static String quoteString(String value) {
		StringBuilder escaped = new StringBuilder(value.length() + 2);
		escaped.append('"');
		for (int i = 0; i < value.length(); i++) {
			char ch = value.charAt(i);
			if (ch == '\\' || ch == '"') {
				escaped.append('\\');
			}
			escaped.append(ch);
		}
		escaped.append('"');
		return escaped.toString();
	}
}
