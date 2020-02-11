/*
 * Copyright 2012-2019 the original author or authors
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

import com.couchbase.client.java.json.JsonValue;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

import static org.springframework.data.couchbase.core.query.N1QLExpression.x;

/**
 * A {@link RepositoryQuery} for Couchbase, based on N1QL and a String statement.
 * <p/>
 * The statement can contain positional placeholders (eg. <code>name = $1</code>) that will map to the
 * method's parameters, in the same order.
 * <p/>
 * The statement can also contain SpEL expressions enclosed in <code>#{</code> and <code>}</code>.
 * <p/>
 * There are couchbase-provided variables included for the {@link StringBasedN1qlQueryParser#SPEL_BUCKET bucket namespace},
 * the {@link StringBasedN1qlQueryParser#SPEL_ENTITY ID and CAS fields} necessary for entity reconstruction
 * or a shortcut that covers {@link StringBasedN1qlQueryParser#SPEL_SELECT_FROM_CLAUSE SELECT AND FROM clauses},
 * along with a variable for {@link StringBasedN1qlQueryParser#SPEL_FILTER WHERE clause filtering} of the correct entity.
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 */
public class StringN1qlBasedQuery extends AbstractN1qlBasedQuery {
  private final SpelExpressionParser parser;
  private final QueryMethodEvaluationContextProvider evaluationContextProvider;
  private final StringBasedN1qlQueryParser queryParser;

  protected String getTypeField() {
    return getCouchbaseOperations().getConverter().getTypeKey();
  }

  protected Class<?> getTypeValue() {
    return getQueryMethod().getEntityInformation().getJavaType();
  }

  public StringN1qlBasedQuery(String statement, CouchbaseQueryMethod queryMethod, CouchbaseOperations couchbaseOperations,
                              SpelExpressionParser spelParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {
    super(queryMethod, couchbaseOperations);
    this.queryParser = new StringBasedN1qlQueryParser(statement, queryMethod,
            getCouchbaseOperations().getBucketName(), getCouchbaseOperations().getConverter(), getTypeField(), getTypeValue());
    this.parser = spelParser;
    this.evaluationContextProvider = evaluationContextProvider;
  }

  @Override
  protected JsonValue getPlaceholderValues(ParameterAccessor accessor) {
    return this.queryParser.getPlaceholderValues(accessor);
  }

  @Override
  public N1QLExpression getExpression(ParameterAccessor accessor, Object[] runtimeParameters, ReturnedType returnedType) {
    EvaluationContext evaluationContext = evaluationContextProvider.getEvaluationContext(getQueryMethod().getParameters(), runtimeParameters);
    N1QLExpression parsedStatement = x(this.queryParser.doParse(parser, evaluationContext, false));


    Sort sort = accessor.getSort();
    if (sort.isSorted()) {
      N1QLExpression[] cbSorts = N1qlUtils.createSort(sort);
      parsedStatement = parsedStatement.orderBy(cbSorts);
    }
    if (queryMethod.isPageQuery()) {
      Pageable pageable = accessor.getPageable();
      Assert.notNull(pageable, "Pageable must not be null!");
      parsedStatement = parsedStatement.limit(pageable.getPageSize())
			  .offset(Math.toIntExact(pageable.getOffset()));
    } else if (queryMethod.isSliceQuery()) {
      Pageable pageable = accessor.getPageable();
      Assert.notNull(pageable, "Pageable must not be null!");
      parsedStatement = parsedStatement.limit(pageable.getPageSize() + 1)
              .offset(Math.toIntExact(pageable.getOffset()));
    }
    return parsedStatement;
  }

  @Override
  protected N1QLExpression getCount(ParameterAccessor accessor, Object[] runtimeParameters) {
    EvaluationContext evaluationContext = evaluationContextProvider.getEvaluationContext(getQueryMethod().getParameters(), runtimeParameters);
    return x(this.queryParser.doParse(parser, evaluationContext, true));
  }

  @Override
  protected boolean useGeneratedCountQuery() {
    return this.queryParser.useGeneratedCountQuery();
  }

}
