/*
 * Copyright 2012-2015 the original author or authors
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

package org.springframework.data.couchbase.repository.query;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.json.JsonValue;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.path.DefaultLimitPath;
import com.couchbase.client.java.query.dsl.path.DefaultOrderByPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

/**
 * A {@link RepositoryQuery} for Couchbase, based on N1QL and a String statement.
 * <p/>
 * The statement can contain positional placeholders (eg. <code>name = $1</code>) that will map to the
 * method's parameters, in the same order.
 * <p/>
 * The statement can also contain SpEL expressions enclosed in <code>#{</code> and <code>}</code>.
 * <p/>
 * There are couchbase-provided variables included for the {@link #SPEL_BUCKET bucket namespace},
 * the {@link #SPEL_ENTITY ID and CAS fields} necessary for entity reconstruction
 * or a shortcut that covers {@link #SPEL_SELECT_FROM_CLAUSE SELECT AND FROM clauses},
 * along with a variable for {@link #SPEL_FILTER WHERE clause filtering} of the correct entity.
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 */
public class StringN1qlBasedQuery extends AbstractN1qlBasedQuery {

  private static final Logger LOGGER = LoggerFactory.getLogger(StringN1qlBasedQuery.class);


  public static final String SPEL_PREFIX = "n1ql";

  /**
   * Use this variable in a SpEL expression in a {@link org.springframework.data.couchbase.core.query.Query @Query}
   * annotation's inline statement. This will be replaced by the correct <code>SELECT x FROM y</code> clause needed
   * for entity mapping. Eg. <code>"#{{@value StringN1qlBasedQuery#SPEL_SELECT_FROM_CLAUSE}} WHERE test = true"</code>.
   * Note this only makes sense once, as the beginning of the statement.
   */
  public static final String SPEL_SELECT_FROM_CLAUSE = "#" + SPEL_PREFIX + ".selectEntity";

  /**
   * Use this variable in a SpEL expression in a {@link org.springframework.data.couchbase.core.query.Query @Query}
   * annotation's inline statement. This will be replaced by the (escaped) bucket name corresponding to the repository's
   * entity. Eg. <code>"SELECT * FROM #{{@value StringN1qlBasedQuery#SPEL_BUCKET}} LIMIT 3"</code>.
   */
  public static final String SPEL_BUCKET = "#" + SPEL_PREFIX + ".bucket";

  /**
   * Use this variable in a SpEL expression in a {@link org.springframework.data.couchbase.core.query.Query @Query}
   * annotation's inline statement. This will be replaced by the fields allowing to construct the repository's entity
   * (SELECT clause). Eg. <code>"SELECT #{{@value StringN1qlBasedQuery#SPEL_ENTITY}} FROM test"</code>.
   */
  public static final String SPEL_ENTITY = "#" + SPEL_PREFIX + ".fields";

  /**
   * Use this variable in a SpEL expression in a {@link org.springframework.data.couchbase.core.query.Query @Query}
   * annotation's inline statement WHERE clause. This will be replaced by the expression allowing to only select
   * documents matching the entity's class. Eg. <code>"SELECT * FROM test WHERE test = true AND #{{@value StringN1qlBasedQuery#SPEL_FILTER}}"</code>.
   */
  public static final String SPEL_FILTER = "#" + SPEL_PREFIX + ".filter";

  /** regexp that detect $named placeholder (starts with a letter, then alphanum chars) */
  private static final Pattern NAMED_PLACEHOLDER_PATTERN = Pattern.compile("\\W(\\$\\p{Alpha}\\p{Alnum}*)\\b");
  /** regexp that detect positional placeholder ($ followed by digits only) */
  private static final Pattern POSITIONAL_PLACEHOLDER_PATTERN = Pattern.compile("\\W(\\$\\p{Digit}+)\\b");
  /** regexp that detects " and ' quote boundaries, ignoring escaped quotes */
  private static final Pattern QUOTE_DETECTION_PATTERN = Pattern.compile("[\"'](?:[^\"'\\\\]*(?:\\\\.)?)*[\"']");

  /** enumeration of all the combinations of placeholder types that could be found in a N1QL statement */
  private enum PlaceholderType {
    NAMED, POSITIONAL, NONE
  }

  private final String originalStatement;
  private final PlaceholderType placeHolderType;
  private final SpelExpressionParser parser;
  private final EvaluationContextProvider evaluationContextProvider;
  private final N1qlSpelValues countContext;
  private final N1qlSpelValues statementContext;


  protected String getTypeField() {
    return getCouchbaseOperations().getConverter().getTypeKey();
  }

  protected Class<?> getTypeValue() {
    return getQueryMethod().getEntityInformation().getJavaType();
  }

  public StringN1qlBasedQuery(String statement, CouchbaseQueryMethod queryMethod, CouchbaseOperations couchbaseOperations,
                              SpelExpressionParser spelParser, final EvaluationContextProvider evaluationContextProvider) {
    super(queryMethod, couchbaseOperations);
    this.originalStatement = statement;
    this.placeHolderType = checkPlaceholders(statement);

    this.parser = spelParser;
    this.evaluationContextProvider = evaluationContextProvider;

    this.statementContext = createN1qlSpelValues(getCouchbaseOperations().getCouchbaseBucket().name(), getTypeField(), getTypeValue(), false);
    this.countContext = createN1qlSpelValues(getCouchbaseOperations().getCouchbaseBucket().name(), getTypeField(), getTypeValue(), true);
  }

  private PlaceholderType checkPlaceholders(String statement) {
    Matcher quoteMatcher = QUOTE_DETECTION_PATTERN.matcher(statement);
    Matcher positionMatcher = POSITIONAL_PLACEHOLDER_PATTERN.matcher(statement);
    Matcher namedMatcher = NAMED_PLACEHOLDER_PATTERN.matcher(statement);

    List<int[]> quotes = new ArrayList<int[]>();
    while(quoteMatcher.find()) {
      quotes.add(new int[] { quoteMatcher.start(), quoteMatcher.end() });
    }

    int posCount = 0;
    int namedCount = 0;

    while(positionMatcher.find()) {
      String placeholder = positionMatcher.group(1);
      //check not in quoted
      if (checkNotQuoted(placeholder, positionMatcher.start(), positionMatcher.end(), quotes)) {
        LOGGER.trace("{}: Found positional placeholder {}", getQueryMethod().getName(), placeholder);
        posCount++;
      }
    }

    while(namedMatcher.find()) {
      String placeholder = namedMatcher.group(1);
      //check not in quoted
      if (checkNotQuoted(placeholder, namedMatcher.start(), namedMatcher.end(), quotes)) {
        LOGGER.trace("{}: Found named placeholder {}", getQueryMethod().getName(), placeholder);
        namedCount++;
      }
    }

    if (posCount > 0 && namedCount > 0) {
      throw new IllegalArgumentException("Using both named (" + namedCount + ") and positional (" + posCount +
          ") placeholders is not supported, please choose one over the other in " + queryMethod.getName());
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
        LOGGER.trace("{}: potential placeholder {} is inside quotes, ignored", queryMethod.getName(), item);
        return false;
      }
    }
    return true;
  }

  public static N1qlSpelValues createN1qlSpelValues(String bucketName, String typeField, Class<?> typeValue, boolean isCount) {
    String b = "`" + bucketName + "`";
    String entity = "META(" + b + ").id AS " + CouchbaseOperations.SELECT_ID +
        ", META(" + b + ").cas AS " + CouchbaseOperations.SELECT_CAS;
    String count = "COUNT(*) AS " + CountFragment.COUNT_ALIAS;
    String selectEntity;
    if (isCount) {
      selectEntity = "SELECT " + count + " FROM " + b;
    } else {
      selectEntity = "SELECT " + entity + ", " + b + ".* FROM " + b;
    }
    String typeSelection = "`" + typeField + "` = \"" + typeValue.getName() + "\"";

    return new N1qlSpelValues(selectEntity, entity, b, typeSelection);
  }

  /**
   * Parse the statement to detect SPEL blocks (delimited by <code>#{</code> and <code>}</code>)
   * and replace said expression blocks with their corresponding values.
   *
   * @param statement the full statement into which SpEL expressions should be parsed and replaced.
   * @param runtimeParameters the parameters passed into the method at runtime.
   * @return the statement with the SpEL interpreted and replaced by its values.
   */
  protected String parseSpel(String statement, boolean isCount, Object[] runtimeParameters) {
    EvaluationContext evaluationContext = evaluationContextProvider.getEvaluationContext(getQueryMethod().getParameters(), runtimeParameters);
    N1qlSpelValues n1qlSpelValues = this.statementContext;
    if (isCount) {
      n1qlSpelValues = this.countContext;
    }
    return doParse(statement, parser, evaluationContext, n1qlSpelValues);
  }

  //this static method can be used to test the parsing behavior for Couchbase specific spel variables
  //in isolation from the rest of the spel parser initialization chain.
  protected static String doParse(String statement, SpelExpressionParser parser, EvaluationContext evaluationContext, N1qlSpelValues n1qlSpelValues) {
    Expression parsedExpression = parser.parseExpression(statement, new TemplateParserContext());
    evaluationContext.setVariable(SPEL_PREFIX, n1qlSpelValues);
    return parsedExpression.getValue(evaluationContext, String.class);
  }

  @Override
  protected JsonValue getPlaceholderValues(ParameterAccessor accessor) {
    switch (this.placeHolderType) {
      case NAMED:
        return getNamedPlaceholderValues(accessor);
      case POSITIONAL:
        return getPositionalPlaceholderValues(accessor);
      case NONE:
      default:
        return JsonArray.empty();
    }
  }

  private JsonValue getPositionalPlaceholderValues(ParameterAccessor accessor) {
    JsonArray posValues = JsonArray.create();
    for (Parameter parameter : getQueryMethod().getParameters().getBindableParameters()) {
      posValues.add(accessor.getBindableValue(parameter.getIndex()));
    }
    return posValues;
  }

  private JsonObject getNamedPlaceholderValues(ParameterAccessor accessor) {
    JsonObject namedValues = JsonObject.create();

    for (Parameter parameter : getQueryMethod().getParameters().getBindableParameters()) {
      String placeholder = parameter.getPlaceholder();
      Object value = accessor.getBindableValue(parameter.getIndex());

      if (placeholder != null && placeholder.charAt(0) == ':') {
        placeholder = placeholder.replaceFirst(":", "");
        namedValues.put(placeholder, value);
      } else {
        namedValues.put(parameter.getName(), value);
      }
    }
    return namedValues;
  }

  @Override
  public Statement getStatement(ParameterAccessor accessor, Object[] runtimeParameters, ReturnedType returnedType) {
    String parsedStatement = parseSpel(this.originalStatement, false, runtimeParameters);

    String orderByPart = "";
    String limitByPart = "";

    Sort sort = accessor.getSort();
    if (sort != null) {
      com.couchbase.client.java.query.dsl.Sort[] cbSorts = N1qlUtils.createSort(sort, getCouchbaseOperations().getConverter());
      orderByPart = " " + new DefaultOrderByPath(null).orderBy(cbSorts).toString();
    }
    if (queryMethod.isPageQuery()) {
      Pageable pageable = accessor.getPageable();
      Assert.notNull(pageable);
      limitByPart = " " + new DefaultLimitPath(null).limit(pageable.getPageSize()).offset(pageable.getOffset()).toString();
    } else if (queryMethod.isSliceQuery()) {
      Pageable pageable = accessor.getPageable();
      Assert.notNull(pageable);
      limitByPart = " " + new DefaultLimitPath(null).limit(pageable.getPageSize() + 1).offset(pageable.getOffset()).toString();
    }
    return N1qlQuery.simple(parsedStatement + orderByPart + limitByPart).statement();
  }

  @Override
  protected Statement getCount(ParameterAccessor accessor, Object[] runtimeParameters) {
    String parsedCountStatement = parseSpel(this.originalStatement, true, runtimeParameters);
    return N1qlQuery.simple(parsedCountStatement).statement();
  }

  @Override
  protected boolean useGeneratedCountQuery() {
    return this.originalStatement.contains(SPEL_SELECT_FROM_CLAUSE);
  }

  /**
   * This class is exposed to SpEL parsing through the variable <code>#{@value StringN1qlBasedQuery#SPEL_PREFIX}</code>.
   * Use the attributes in your SpEL expressions: {@link #selectEntity}, {@link #fields}, {@link #bucket} and {@link #filter}.
   */
  public static final class N1qlSpelValues {

    /**
     * <code>#{{@value org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery#SPEL_PREFIX}.
     * selectEntity</code> will be replaced by the SELECT-FROM clause corresponding to the entity. Use once at the beginning.
     */
    public final String selectEntity;

    /**
     * <code>#{{@value org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery#SPEL_PREFIX}.
     * fields</code> will be replaced by the list of N1QL fields allowing to reconstruct the entity.
     */
    public final String fields;

    /**
     * <code>#{{@value org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery#SPEL_PREFIX}.
     * bucket</code> will be replaced by (escaped) bucket name in which the entity is stored.
     */
    public final String bucket;

    /**
     * <code>#{{@value org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery#SPEL_PREFIX}.
     * filter</code> will be replaced by an expression allowing to select only entries matching the entity in a WHERE clause.
     */
    public final String filter;

    public N1qlSpelValues(String selectClause, String entityFields, String bucket, String filter) {
      this.selectEntity = selectClause;
      this.fields = entityFields;
      this.bucket = bucket;
      this.filter = filter;
    }
  }
}
