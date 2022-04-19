/*
 * Copyright 2017-2022 the original author or authors.
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

import static org.springframework.data.couchbase.core.query.N1QLExpression.i;
import static org.springframework.data.couchbase.core.query.N1QLExpression.x;
import static org.springframework.data.couchbase.core.support.TemplateUtils.SELECT_CAS;
import static org.springframework.data.couchbase.core.support.TemplateUtils.SELECT_ID;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.Expiration;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;

/**
 * @author Subhashni Balakrishnan
 * @author Michael Reiche
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
	 * Use this variable in a SpEL expression in a {@link Query @Query} annotation's inline statement WHERE clause. This
	 * will be replaced by the expression allowing to only select documents matching the entity's class. Eg.
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
	/**
	 * regexp that detect $named placeholder (starts with a letter, then alphanum chars)
	 */
	public static final Pattern NAMED_PLACEHOLDER_PATTERN = Pattern.compile("\\W(\\$\\p{Alpha}\\p{Alnum}*)\\b");
	/**
	 * regexp that detect positional placeholder ($ followed by digits only)
	 */
	public static final Pattern POSITIONAL_PLACEHOLDER_PATTERN = Pattern.compile("\\W(\\$\\p{Digit}+)\\b");
	/**
	 * regexp that detects " and ' quote boundaries, ignoring escaped quotes
	 */
	public static final Pattern QUOTE_DETECTION_PATTERN = Pattern.compile("[\"'](?:[^\"'\\\\]*(?:\\\\.)?)*[\"']");
	private static final Logger LOGGER = LoggerFactory.getLogger(StringBasedN1qlQueryParser.class);
	private final String statement;
	private final CouchbaseQueryMethod queryMethod;
	private PlaceholderType placeHolderType;
	private final N1qlSpelValues statementContext;
	private final N1qlSpelValues countContext;
	private final CouchbaseConverter couchbaseConverter;
	private final Collection<String> parameterNames = new HashSet<String>();
	public final N1QLExpression parsedExpression;

	public StringBasedN1qlQueryParser(String statement, CouchbaseQueryMethod queryMethod, String bucketName,
			CouchbaseConverter couchbaseConverter, String typeField, String typeValue, ParameterAccessor accessor,
			SpelExpressionParser parser, QueryMethodEvaluationContextProvider evaluationContextProvider) {
		this.statement = statement;
		this.queryMethod = queryMethod;
		this.couchbaseConverter = couchbaseConverter;
		String collection = queryMethod.getCollection();
		this.statementContext = createN1qlSpelValues(bucketName, collection,
				queryMethod.getEntityInformation().getJavaType(), queryMethod.getReturnedObjectType(), typeField, typeValue,
				false, null, null);
		this.countContext = createN1qlSpelValues(bucketName, collection, queryMethod.getEntityInformation().getJavaType(),
				queryMethod.getReturnedObjectType(), typeField, typeValue, true, null, null);
		this.parsedExpression = getExpression(accessor, null, parser, evaluationContextProvider);
		checkPlaceholders(this.parsedExpression.toString());
	}

	public StringBasedN1qlQueryParser(String bucketName, String collection, CouchbaseConverter couchbaseConverter,
			Class domainClass, Class resultClass, String typeField, String typeValue, boolean isCount,
			String[] distinctFields, String[] fields) {
		this.statement = null;
		this.queryMethod = null;
		this.couchbaseConverter = couchbaseConverter;
		if (!isCount) {
			this.statementContext = createN1qlSpelValues(bucketName, collection, domainClass, resultClass, typeField,
					typeValue, false, distinctFields, fields);
			this.countContext = null;
		} else {
			this.statementContext = null;
			this.countContext = createN1qlSpelValues(bucketName, collection, domainClass, resultClass, typeField, typeValue,
					true, distinctFields, fields);
		}
		this.parsedExpression = null;
	}

	public N1qlSpelValues createN1qlSpelValues(String bucketName, String collection, Class domainClass, Class resultClass,
			String typeField, String typeValue, boolean isCount, String[] distinctFields, String[] fields) {
		String b = collection != null ? collection : bucketName;
		Assert.isTrue(!(distinctFields != null && fields != null),
				"only one of project(fields) and distinct(distinctFields) can be specified");
		String entityFields = "";
		String selectEntity;
		if (distinctFields != null) {
			String distinctFieldsStr = getProjectedOrDistinctFields(b, domainClass, typeField, fields, distinctFields);
			if (isCount) {
				selectEntity = "SELECT COUNT( DISTINCT {" + distinctFieldsStr + "} ) " + CountFragment.COUNT_ALIAS + " FROM "
						+ i(b);
			} else {
				selectEntity = "SELECT DISTINCT " + distinctFieldsStr + " FROM " + i(b);
			}
		} else if (isCount) {
			selectEntity = "SELECT " + "COUNT(*) AS " + CountFragment.COUNT_ALIAS + " FROM " + i(b);
		} else {
			String projectedFields = getProjectedOrDistinctFields(b, domainClass, typeField, fields, distinctFields);
			entityFields = projectedFields;
			selectEntity = "SELECT " + projectedFields + " FROM " + i(b);
		}
		String typeSelection = "`" + typeField + "` = \"" + typeValue + "\"";

		String delete = N1QLExpression.delete().from(b).toString();
		String returning = " returning " + N1qlUtils.createReturningExpressionForDelete(b).toString();

		return new N1qlSpelValues(selectEntity, entityFields, i(b).toString(), typeSelection, delete, returning);
	}

	private String getProjectedOrDistinctFields(String b, Class resultClass, String typeField, String[] fields,
			String[] distinctFields) {
		if (distinctFields != null && distinctFields.length != 0) {
			return i(distinctFields).toString();
		}
		String projectedFields;
		if (resultClass != null && !Modifier.isAbstract(resultClass.getModifiers())) {
			PersistentEntity persistentEntity = couchbaseConverter.getMappingContext().getPersistentEntity(resultClass);
			StringBuilder sb = new StringBuilder();
			getProjectedFieldsInternal(b, null, sb, persistentEntity, typeField, fields, distinctFields != null);
			projectedFields = sb.toString();
		} else {
			projectedFields = i(b) + ".*, " + "META(`" + b + "`).id AS " + SELECT_ID + ", META(`" + b + "`).cas AS "
					+ SELECT_CAS; // if we can't get further information of the fields needed, then project everything
		}
		return projectedFields;
	}

	private void getProjectedFieldsInternal(String bucketName, CouchbasePersistentProperty parent, StringBuilder sb,
			PersistentEntity persistentEntity, String typeField, String[] fields, boolean forDistinct) {

		sb.append(i(typeField));

		if (persistentEntity != null) {
			Set<String> fieldList = fields != null ? new HashSet<>(Arrays.asList(fields)) : null;

			// do not include the id and cas metadata fields.

			persistentEntity.doWithProperties((PropertyHandler<CouchbasePersistentProperty>) prop -> {
				if (prop == persistentEntity.getIdProperty() && parent == null) {
					if (forDistinct) {
						return;
					}
					if (sb.length() > 0) {
						sb.append(", ");
					}
					PersistentPropertyPath<CouchbasePersistentProperty> path = couchbaseConverter.getMappingContext()
							.getPersistentPropertyPath(prop.getName(), persistentEntity.getTypeInformation().getType());
					String projectField = N1qlQueryCreator.addMetaIfRequired(bucketName, path, prop, persistentEntity).toString();
					sb.append(projectField + " AS " + SELECT_ID);
					if (fieldList != null) {
						fieldList.remove(prop.getFieldName());
					}
					return;
				}
				if (prop == persistentEntity.getVersionProperty() && parent == null) {
					if (forDistinct) {
						return;
					}
					if (sb.length() > 0) {
						sb.append(", ");
					}
					PersistentPropertyPath<CouchbasePersistentProperty> path = couchbaseConverter.getMappingContext()
							.getPersistentPropertyPath(prop.getName(), persistentEntity.getTypeInformation().getType());
					String projectField = N1qlQueryCreator.addMetaIfRequired(bucketName, path, prop, persistentEntity).toString();
					sb.append(projectField + " AS " + SELECT_CAS);
					if (fieldList != null) {
						fieldList.remove(prop.getFieldName());
					}
					return;
				}
				if (prop.getFieldName().equals(typeField)) // typeField already projected
					return;
				// for distinct when no distinctFields were provided, do not include the expiration field.
				if (forDistinct && prop.findAnnotation(Expiration.class) != null && parent == null) {
					return;
				}
				String projectField = null;

				if (fieldList == null || fieldList.contains(prop.getFieldName())) {
					PersistentPropertyPath<CouchbasePersistentProperty> path = couchbaseConverter.getMappingContext()
							.getPersistentPropertyPath(prop.getName(), persistentEntity.getTypeInformation().getType());
					projectField = N1qlQueryCreator.addMetaIfRequired(bucketName, path, prop, persistentEntity).toString();
					if (sb.length() > 0) {
						sb.append(", ");
					}
					sb.append(projectField); // from N1qlQueryCreator
				}

				if (fieldList != null) {
					fieldList.remove(prop.getFieldName());
				}
				// The current limitation is that only top-level properties can be projected
				// This traversing of nested data structures would need to replicate the processing done by
				// MappingCouchbaseConverter. Either the read or write
				// And the n1ql to project lower-level properties is complex

				// if (!conversions.isSimpleType(prop.getType())) {
				// getProjectedFieldsInternal(prop, sb, prop.getTypeInformation(), path+prop.getName()+".");
				// } else {
				// }
			});
			// throw an exception if there is an request for a field not in the entity.
			// needs further discussion as removing a field from an entity could cause this and not necessarily be an error
			if (fieldList != null && !fieldList.isEmpty()) {
				throw new CouchbaseException(
						"projected fields (" + fieldList + ") not found in entity: " + persistentEntity.getName());
			}
		} else {
			for (String field : fields) {
				if (!field.equals(typeField)) { // typeField is already projected
					if (sb.length() > 0) {
						sb.append(", ");
					}
				}
				sb.append(x(field));
			}

		}
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

	private void checkPlaceholders(String statement) {

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
				parameterNames.add(placeholder.substring(1)); // save without the leading $
			}
		}

		while (namedMatcher.find()) {
			String placeholder = namedMatcher.group(1);
			// check not in quoted
			if (checkNotQuoted(placeholder, namedMatcher.start(), namedMatcher.end(), quotes)) {
				LOGGER.trace("{}: Found named placeholder {}", this.queryMethod.getName(), placeholder);
				namedCount++;
				parameterNames.add(placeholder.substring(1));// save without the leading $
			}
		}

		if (posCount > 0 && namedCount > 0) { // actual values from parameterNames might be more useful
			throw new IllegalArgumentException("Using both named (" + namedCount + ") and positional (" + posCount
					+ ") placeholders is not supported, please choose one over the other in " + queryMethod.getClass().getName()
					+ "." + this.queryMethod.getName() + "()");
		}

		if (posCount > 0) {
			placeHolderType = PlaceholderType.POSITIONAL;
		} else if (namedCount > 0) {
			placeHolderType = PlaceholderType.NAMED;
		} else {
			placeHolderType = PlaceholderType.NONE;
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

	/**
	 * Get Postional argument placeholders to use for parameters. $1, $2 etc.
	 *
	 * @param accessor
	 * @return - JsonValue holding parameters.
	 */
	private JsonValue getPositionalPlaceholderValues(ParameterAccessor accessor) {
		JsonArray posValues = JsonArray.create();
		for (Parameter parameter : this.queryMethod.getParameters().getBindableParameters()) {
			Object rawValue = accessor.getBindableValue(parameter.getIndex());
			Object value = couchbaseConverter.convertForWriteIfNeeded(rawValue);
			putPositionalValue(posValues, value);
		}
		return posValues;
	}

	/**
	 * Get Named argument placeholders to use for parameters. $lastname, $city etc.
	 *
	 * @param accessor
	 * @return
	 */
	private JsonObject getNamedPlaceholderValues(ParameterAccessor accessor) {
		JsonObject namedValues = JsonObject.create();
		HashSet<String> pNames = new HashSet<>(parameterNames);
		for (Parameter parameter : this.queryMethod.getParameters().getBindableParameters()) {
			String placeholder = parameter.getPlaceholder();
			Object rawValue = accessor.getBindableValue(parameter.getIndex());
			Object value = couchbaseConverter.convertForWriteIfNeeded(rawValue);

			if (placeholder != null && placeholder.charAt(0) == ':') {
				placeholder = placeholder.replaceFirst(":", "");
				putNamedValue(namedValues, placeholder, value);
				if (pNames.contains(placeholder)) {
					pNames.remove(placeholder);
				} else {
					throw new RuntimeException("parameter named " + placeholder + " does not match any named parameter "
							+ parameterNames + " in " + statement);
				}
			} else {
				if (parameter.getName().isPresent()) {
					putNamedValue(namedValues, parameter.getName().get(), value);
				} else {
					throw new RuntimeException("cannot determine argument for named parameter. " + "Argument "
							+ parameter.getIndex() + " to " + queryMethod.getClass().getName() + "." + queryMethod.getName()
							+ "() needs @Param(\"name\") that matches a named parameter in " + statement);
				}
			}
		}
		if (!pNames.isEmpty()) {
			throw new RuntimeException("no parameter found for " + pNames);
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

	private void putPositionalValue(JsonArray posValues, Object value) {
		try {
			posValues.add(value);
		} catch (InvalidArgumentException iae) {
			if (value instanceof Object[]) { // Maybe just need to treat it as an array ?
				addAsArray(posValues, value);
			} else {
				throw iae;
			}
		}
	}

	private void addAsArray(JsonArray posValues, Object o) {
		Object[] array = (Object[]) o;
		JsonArray ja = JsonValue.ja();
		for (Object e : array) {
			ja.add(String.valueOf(couchbaseConverter.convertForWriteIfNeeded(e)));
		}
		posValues.add(ja);
	}

	private void putNamedValue(JsonObject namedValues, String placeholder, Object value) {
		try {
			namedValues.put(placeholder, value);
		} catch (InvalidArgumentException iae) {
			if (value instanceof Object[]) { // Maybe just need to treat it as an array?
				addAsArray(namedValues, placeholder, value);
			} else {
				throw iae;
			}
		}
	}

	private void addAsArray(JsonObject namedValues, String placeholder, Object o) {
		Object[] array = (Object[]) o;
		JsonArray ja = JsonValue.ja();
		for (Object e : array) {
			ja.add(String.valueOf(couchbaseConverter.convertForWriteIfNeeded(e)));
		}
		namedValues.put(placeholder, ja);
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

	/**
	 * enumeration of all the combinations of placeholder types that could be found in a N1QL statement
	 */
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

	// copied from StringN1qlBasedQuery
	private N1QLExpression getExpression(ParameterAccessor accessor, ReturnedType returnedType,
			SpelExpressionParser parser, QueryMethodEvaluationContextProvider evaluationContextProvider) {
		boolean isCountQuery = queryMethod.isCountQuery();
		Object[] runtimeParameters = getParameters(accessor);
		EvaluationContext evaluationContext = evaluationContextProvider.getEvaluationContext(queryMethod.getParameters(),
				runtimeParameters);
		N1QLExpression parsedStatement = x(this.doParse(parser, evaluationContext, isCountQuery));
		return parsedStatement;
	}

	private static Object[] getParameters(ParameterAccessor accessor) {
		ArrayList<Object> params = new ArrayList<>();
		for (Object o : accessor) {
			params.add(o);
		}
		params.add(accessor.getPageable());
		params.add(accessor.getSort());
		return params.toArray();
	}
}
