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
package org.springframework.data.couchbase.repository.query;

import static org.springframework.data.couchbase.core.query.N1QLExpression.*;
import static org.springframework.data.couchbase.core.support.TemplateUtils.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;

/**
 * @author Subhashni Balakrishnan
 */
public class StringBasedN1qlQueryParser {
	public static final String SPEL_PREFIX = "n1ql";
	/**
	 * Use this variable in a SpEL expression in a {@link Query @Query} annotation's inline statement. This will be
	 * replaced by the correct <code>SELECT x FROM y</code> clause needed for entity mapping. Eg.
	 * <code>"#{{@value SPEL_SELECT_FROM_CLAUSE}} WHERE test = true"</code>. Note this only makes sense once, as the
	 * beginning of the statement.
	 */
	public static final String SPEL_SELECT_FROM_CLAUSE = "#" + SPEL_PREFIX + ".selectEntity";
	/**
	 * Use this variable in a SpEL expression in a {@link Query @Query} annotation's inline statement. This will be
	 * replaced by the (escaped) bucket name corresponding to the repository's entity. Eg.
	 * <code>"SELECT * FROM #{{@value SPEL_BUCKET}} LIMIT 3"</code>.
	 */
	public static final String SPEL_BUCKET = "#" + SPEL_PREFIX + ".bucket";
	/**
	 * Use this variable in a SpEL expression in a {@link Query @Query} annotation's inline statement. This will be
	 * replaced by the fields allowing to construct the repository's entity (SELECT clause). Eg.
	 * <code>"SELECT #{{@value SPEL_ENTITY}} FROM test"</code>.
	 */
	public static final String SPEL_ENTITY = "#" + SPEL_PREFIX + ".fields";
	/**
	 * Use this variable in a SpEL expression in a {@link Query @Query} annotation's inline statement WHERE clause.
	 * This will be replaced by the expression allowing to only select documents matching the entity's class. Eg.
	 * <code>"SELECT * FROM test WHERE test = true AND #{{@value SPEL_FILTER}}"</code>.
	 */
	public static final String SPEL_FILTER = "#" + SPEL_PREFIX + ".filter";
	/**
	 * Use this variable in a SpEL expression in a {@link Query @Query} annotation's inline statement. This will be
	 * replaced by the correct <code>delete</code> expression needed Eg.
	 * <code>"#{{@value SPEL_DELETE}} WHERE test = true"</code>. Note this only makes sense once, as the beginning of the
	 * statement.
	 */
	public static final String SPEL_DELETE = "#" + SPEL_PREFIX + ".delete";
	/**
	 * Use this variable in a SpEL expression in a {@link Query @Query} annotation's inline statement. This will be
	 * replaced by the correct <code>returning</code> clause needed for entity mapping. Eg.
	 * <code>"#{{@value SPEL_RETURNING}} WHERE test = true"</code>. Note this only makes sense once, as the beginning of
	 * the statement.
	 */
	public static final String SPEL_RETURNING = "#" + SPEL_PREFIX + ".returning";
	/** regexp that detect $named placeholder (starts with a letter, then alphanum chars) */
	public static final Pattern NAMED_PLACEHOLDER_PATTERN = Pattern.compile("\\W(\\$\\p{Alpha}\\p{Alnum}*)\\b");
	/** regexp that detect positional placeholder ($ followed by digits only) */
	public static final Pattern POSITIONAL_PLACEHOLDER_PATTERN = Pattern.compile("\\W(\\$\\p{Digit}+)\\b");
	/** regexp that detects " and ' quote boundaries, ignoring escaped quotes */
	public static final Pattern QUOTE_DETECTION_PATTERN = Pattern.compile("[\"'](?:[^\"'\\\\]*(?:\\\\.)?)*[\"']");
	private static final Logger LOGGER = LoggerFactory.getLogger(StringBasedN1qlQueryParser.class);
	private final String statement;
	private final QueryMethod queryMethod;
	private final PlaceholderType placeHolderType;
	private final N1qlSpelValues statementContext;
	private final N1qlSpelValues countContext;
	private final CouchbaseConverter couchbaseConverter;
	public StringBasedN1qlQueryParser(String statement, QueryMethod queryMethod, String bucketName,
			CouchbaseConverter couchbaseConverter, String typeField, Class<?> typeValue) {
		this.statement = statement;
		this.queryMethod = queryMethod;
		this.placeHolderType = checkPlaceholders(statement);
		this.statementContext = createN1qlSpelValues(bucketName, typeField, typeValue, false);
		this.countContext = createN1qlSpelValues(bucketName, typeField, typeValue, true);
		this.couchbaseConverter = couchbaseConverter;
	}

	public static N1qlSpelValues createN1qlSpelValues(String bucketName, String typeField, Class<?> typeValue,
			boolean isCount) {
		String b = "`" + bucketName + "`";
		String entity = "META(" + b + ").id AS " + SELECT_ID + ", META(" + b + ").cas AS " + SELECT_CAS;
		String count = "COUNT(*) AS " + CountFragment.COUNT_ALIAS;
		String selectEntity;
		if (isCount) {
			selectEntity = "SELECT " + count + " FROM " + b;
		} else {
			selectEntity = "SELECT " + entity + ", " + b + ".* FROM " + b;
		}
		String typeSelection = "`" + typeField + "` = \"" + typeValue.getName() + "\"";

		String delete = N1QLExpression.delete().from(i(bucketName)).toString();
		String returning = " returning " + N1qlUtils.createReturningExpressionForDelete(bucketName).toString();

		return new N1qlSpelValues(selectEntity, entity, b, typeSelection, delete, returning);
	}

	// this static method can be used to test the parsing behavior for Couchbase specific spel variables
	// in isolation from the rest of the spel parser initialization chain.
	public String doParse(SpelExpressionParser parser, EvaluationContext evaluationContext, boolean isCountQuery) {
		org.springframework.expression.Expression parsedExpression = parser.parseExpression(this.getStatement(),
				new TemplateParserContext());
		if (isCountQuery) {
			evaluationContext.setVariable(SPEL_PREFIX, this.getCountContext());
		} else {
			evaluationContext.setVariable(SPEL_PREFIX, this.getStatementContext());
		}
		return parsedExpression.getValue(evaluationContext, String.class);
	}

	private PlaceholderType checkPlaceholders(String statement) {
		Matcher quoteMatcher = QUOTE_DETECTION_PATTERN.matcher(statement);
		Matcher positionMatcher = POSITIONAL_PLACEHOLDER_PATTERN.matcher(statement);
		Matcher namedMatcher = NAMED_PLACEHOLDER_PATTERN.matcher(statement);

		List<int[]> quotes = new ArrayList<int[]>();
		while (quoteMatcher.find()) {
			quotes.add(new int[] { quoteMatcher.start(), quoteMatcher.end() });
		}

		int posCount = 0;
		int namedCount = 0;

		while (positionMatcher.find()) {
			String placeholder = positionMatcher.group(1);
			// check not in quoted
			if (checkNotQuoted(placeholder, positionMatcher.start(), positionMatcher.end(), quotes)) {
				LOGGER.trace("{}: Found positional placeholder {}", this.queryMethod.getName(), placeholder);
				posCount++;
			}
		}

		while (namedMatcher.find()) {
			String placeholder = namedMatcher.group(1);
			// check not in quoted
			if (checkNotQuoted(placeholder, namedMatcher.start(), namedMatcher.end(), quotes)) {
				LOGGER.trace("{}: Found named placeholder {}", this.queryMethod.getName(), placeholder);
				namedCount++;
			}
		}

		if (posCount > 0 && namedCount > 0) {
			throw new IllegalArgumentException("Using both named (" + namedCount + ") and positional (" + posCount
					+ ") placeholders is not supported, please choose one over the other in " + this.queryMethod.getName());
		} else if (posCount > 0) {
			return PlaceholderType.POSITIONAL;
		} else if (namedCount > 0) {
			return PlaceholderType.NAMED;
		} else {
			return PlaceholderType.NONE;
		}
	}

	private boolean checkNotQuoted(String item, int start, int end, List<int[]> quotes) {
		for (int[] quote : quotes) {
			if (quote[0] <= start && quote[1] >= end) {
				LOGGER.trace("{}: potential placeholder {} is inside quotes, ignored", this.queryMethod.getName(), item);
				return false;
			}
		}
		return true;
	}

	private JsonValue getPositionalPlaceholderValues(ParameterAccessor accessor) {
		JsonArray posValues = JsonArray.create();
		for (Parameter parameter : this.queryMethod.getParameters().getBindableParameters()) {
			posValues.add(this.couchbaseConverter.convertForWriteIfNeeded(accessor.getBindableValue(parameter.getIndex())));
		}
		return posValues;
	}

	private JsonObject getNamedPlaceholderValues(ParameterAccessor accessor) {
		JsonObject namedValues = JsonObject.create();

		for (Parameter parameter : this.queryMethod.getParameters().getBindableParameters()) {
			String placeholder = parameter.getPlaceholder();
			Object value = this.couchbaseConverter.convertForWriteIfNeeded(accessor.getBindableValue(parameter.getIndex()));

			if (placeholder != null && placeholder.charAt(0) == ':') {
				placeholder = placeholder.replaceFirst(":", "");
				namedValues.put(placeholder, value);
			} else {
				parameter.getName().ifPresent(name -> namedValues.put(name, value));
			}
		}
		return namedValues;
	}

	protected JsonValue getPlaceholderValues(ParameterAccessor accessor) {
		switch (this.placeHolderType) {
			case NAMED:
				return getNamedPlaceholderValues(accessor);
			case POSITIONAL:
				return getPositionalPlaceholderValues(accessor);
			case NONE:
			default:
				return JsonArray.create();
		}
	}

	protected boolean useGeneratedCountQuery() {
		return this.statement.contains(SPEL_SELECT_FROM_CLAUSE);
	}

	public N1qlSpelValues getCountContext() {
		return this.countContext;
	}

	public N1qlSpelValues getStatementContext() {
		return this.statementContext;
	}

	public String getStatement() {
		return this.statement;
	}

	/** enumeration of all the combinations of placeholder types that could be found in a N1QL statement */
	private enum PlaceholderType {
		NAMED, POSITIONAL, NONE
	}

	/**
	 * This class is exposed to SpEL parsing through the variable <code>#{@value SPEL_PREFIX}</code>. Use the attributes
	 * in your SpEL expressions: {@link #selectEntity}, {@link #fields}, {@link #bucket} and {@link #filter}.
	 */
	public static final class N1qlSpelValues {

		/**
		 * <code>#{{@value SPEL_SELECT_FROM_CLAUSE}.
		 * selectEntity</code> will be replaced by the SELECT-FROM clause corresponding to the entity. Use once at the
		 * beginning.
		 */
		public final String selectEntity;

		/**
		 * <code>#{{@value SPEL_ENTITY}.
		 * fields</code> will be replaced by the list of N1QL fields allowing to reconstruct the entity.
		 */
		public final String fields;

		/**
		 * <code>#{{@value SPEL_BUCKET}.
		 * bucket</code> will be replaced by (escaped) bucket name in which the entity is stored.
		 */
		public final String bucket;

		/**
		 * <code>#{{@value SPEL_FILTER}}.
		 * filter</code> will be replaced by an expression allowing to select only entries matching the entity in a WHERE
		 * clause.
		 */
		public final String filter;

		/**
		 * <code>#{{@value SPEL_DELETE}}.
		 * delete</code> will be replaced by a delete expression.
		 */
		public final String delete;

		/**
		 * <code>#{{@value SPEL_RETURNING}}.
		 * returning</code> will be replaced by a returning expression allowing to return the entity and meta information on
		 * deletes.
		 */
		public final String returning;

		public N1qlSpelValues(String selectClause, String entityFields, String bucket, String filter, String delete,
				String returning) {
			this.selectEntity = selectClause;
			this.fields = entityFields;
			this.bucket = bucket;
			this.filter = filter;
			this.delete = delete;
			this.returning = returning;
		}
	}
}
