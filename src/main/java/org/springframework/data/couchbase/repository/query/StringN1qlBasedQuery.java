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

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * A {@link RepositoryQuery} for Couchbase, based on N1QL and a String statement.
 * <p/>
 * The statement can contain positional placeholders (eg. <code>name = $1</code>) that will map to the
 * method's parameters, in the same order.
 * <p/>
 * The statement can also contain SpEL expressions enclosed in <code>${}</code>.
 * <p/> There are couchbase-provided variables included for the {@link #SPEL_BUCKET bucket namespace},
 * the {@link #SPEL_ENTITY ID and CAS fields} necessary for entity reconstruction
 * or a shortcut that covers {@link #SPEL_SELECT_FROM_CLAUSE SELECT AND FROM clauses},
 * along with a variable for {@link #SPEL_FILTER WHERE clause filtering} of the correct entity.
 *
 * @author Simon Baslé
 */
public class StringN1qlBasedQuery extends AbstractN1qlBasedQuery {

  private static final Logger LOGGER = LoggerFactory.getLogger(StringN1qlBasedQuery.class);


  public static final String SPEL_PREFIX = "n1ql";

  /**
   * Use this variable in a SpEL expression in a {@link org.springframework.data.couchbase.core.query.Query @Query}
   * annotation's inline statement. This will be replaced by the correct <code>SELECT x FROM y</code> clause needed
   * for entity mapping. Eg. <code>"${{@value StringN1qlBasedQuery#SPEL_SELECT_FROM_CLAUSE}} WHERE test = true"</code>.
   * Note this only makes sense once, as the beginning of the statement.
   */
  public static final String SPEL_SELECT_FROM_CLAUSE = "#" + SPEL_PREFIX + ".selectEntity";

  /**
   * Use this variable in a SpEL expression in a {@link org.springframework.data.couchbase.core.query.Query @Query}
   * annotation's inline statement. This will be replaced by the (escaped) bucket name corresponding to the repository's
   * entity. Eg. <code>"SELECT * FROM ${{@value StringN1qlBasedQuery#SPEL_BUCKET}} LIMIT 3"</code>.
   */
  public static final String SPEL_BUCKET = "#" + SPEL_PREFIX + ".bucket";

  /**
   * Use this variable in a SpEL expression in a {@link org.springframework.data.couchbase.core.query.Query @Query}
   * annotation's inline statement. This will be replaced by the fields allowing to construct the repository's entity
   * (SELECT clause). Eg. <code>"SELECT ${{@value StringN1qlBasedQuery#SPEL_ENTITY}} FROM test"</code>.
   */
  public static final String SPEL_ENTITY = "#" + SPEL_PREFIX + ".fields";

  /**
   * Use this variable in a SpEL expression in a {@link org.springframework.data.couchbase.core.query.Query @Query}
   * annotation's inline statement WHERE clause. This will be replaced by the expression allowing to only select
   * documents matching the entity's class. Eg. <code>"SELECT * FROM test WHERE test = true AND ${{@value StringN1qlBasedQuery#SPEL_FILTER}}"</code>.
   */
  public static final String SPEL_FILTER = "#" + SPEL_PREFIX + ".filter";

  private final String originalStatement;
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
    this.parser = spelParser;
    this.evaluationContextProvider = evaluationContextProvider;

    this.statementContext = createN1qlSpelValues(getCouchbaseOperations().getCouchbaseBucket().name(), getTypeField(), getTypeValue(), false);
    this.countContext = createN1qlSpelValues(getCouchbaseOperations().getCouchbaseBucket().name(), getTypeField(), getTypeValue(), true);
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
   * Parse the statement to detect SPEL expressions (delimited by <code>${</code> and <code>}</code>)
   * and replace said expressions with their corresponding values (see {@link #evaluateSpelExpression(String, boolean, Object[])}).
   *
   * @param statement the full statement into which SpEL expressions should be parsed and replaced.
   * @param runtimeParameters the parameters passed into the method at runtime.
   * @return the statement with the SpEL interpreted and replaced by its values.
   */
  protected String parseSpel(String statement, boolean isCount, Object[] runtimeParameters) {
    StringBuilder parsed = new StringBuilder(statement);

    int currentIndex = 0;
    while(currentIndex > -1 && currentIndex < parsed.length()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Evaluating from index {}, {}", currentIndex, parsed.substring(currentIndex));
      }
      int opening = parsed.indexOf("${", currentIndex);
      if (opening == -1) {
        break;
      } else {
        //find the end of the ${ } expression, eat inner braces
        int closing = opening + 2;
        int curlyBraceOpenCnt = 1;

        while (curlyBraceOpenCnt > 0) {
          switch (parsed.charAt(closing++)) {
            case '{':
              curlyBraceOpenCnt++;
              break;
            case '}':
              curlyBraceOpenCnt--;
              break;
            default:
          }
        }

        //we're now at the end of the expression
        //evaluate and replace
        String expression = parsed.substring(opening + 2, closing - 1);
        String replacement = evaluateSpelExpression(expression, isCount, runtimeParameters);

        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("Replacing SPEL \"{}\" with value \"{}\"", expression, replacement);
        }

        parsed.replace(opening, closing, replacement);

        //move on
        currentIndex = opening + replacement.length();
      }
    }
    return parsed.toString();
  }

  /**
   * This method is in charge of evaluating a single SPEL expression (as delimited by <code>${</code> and <code>}</code>)
   * and returning the corresponding computed value.
   *
   * @param expression the SPEL expression.
   * @param runtimeParameters the parameters using during method call.
   * @return the corresponding value.
   */
  protected String evaluateSpelExpression(String expression, boolean isCount, Object[] runtimeParameters) {
    EvaluationContext evaluationContext = evaluationContextProvider.getEvaluationContext(getQueryMethod().getParameters(), runtimeParameters);
    Expression parsedExpression = parser.parseExpression(expression);
    N1qlSpelValues n1qlSpelValues = this.statementContext;
    if (isCount) {
      n1qlSpelValues = this.countContext;
    }

    return doEvaluate(evaluationContext, parsedExpression, n1qlSpelValues);
  }

  protected static String doEvaluate(EvaluationContext context, Expression parsedExpression, N1qlSpelValues spelValues) {
    context.setVariable(SPEL_PREFIX, spelValues);
    return parsedExpression.getValue(context, String.class);
  }

  @Override
  protected JsonArray getPlaceholderValues(ParameterAccessor accessor) {
    JsonArray values = JsonArray.create();
    for (Object value : accessor) {
      values.add(value);
    }
    return values;
  }

  @Override
  public Statement getStatement(ParameterAccessor accessor, Object[] runtimeParameters) {
    String parsedStatement = parseSpel(this.originalStatement, false, runtimeParameters);
    return N1qlQuery.simple(parsedStatement).statement();
  }

  @Override
  protected Statement getCount(ParameterAccessor accessor, Object[] runtimeParameters) {
    String parsedCountStatement = parseSpel(this.originalStatement, true, runtimeParameters);
    return N1qlQuery.simple(parsedCountStatement).statement();
  }

  /**
   * This class is exposed to SpEL parsing through the variable <code>#{@value StringN1qlBasedQuery#SPEL_PREFIX}</code>.
   * Use the attributes in your SpEL expressions: {@link #selectEntity}, {@link #fields}, {@link #bucket} and {@link #filter}.
   */
  public static final class N1qlSpelValues {

    /**
     * <code>${{@value org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery#SPEL_PREFIX}.
     * selectEntity</code> will be replaced by the SELECT-FROM clause corresponding to the entity. Use once at the beginning.
     */
    public final String selectEntity;

    /**
     * <code>${{@value org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery#SPEL_PREFIX}.
     * fields</code> will be replaced by the list of N1QL fields allowing to reconstruct the entity.
     */
    public final String fields;

    /**
     * <code>${{@value org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery#SPEL_PREFIX}.
     * bucket</code> will be replaced by (escaped) bucket name in which the entity is stored.
     */
    public final String bucket;

    /**
     * <code>${{@value org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery#SPEL_PREFIX}.
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
