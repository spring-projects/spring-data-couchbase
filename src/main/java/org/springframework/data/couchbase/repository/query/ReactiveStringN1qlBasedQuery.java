/*
 * Copyright 2017-2019 the original author or authors.
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

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.couchbase.client.java.json.JsonValue;

import static org.springframework.data.couchbase.core.query.N1QLExpression.x;


/**
 * A reactive StringN1qlBasedQuery {@link RepositoryQuery} for Couchbase, based on N1QL and a String statement.
 * <p/>
 * The statement can contain positional placeholders (eg. <code>name = $1</code>) that will map to the
 * method's parameters, in the same order.
 * <p/>
 * The statement can also contain SpEL expressions enclosed in <code>#{</code> and <code>}</code>.
 * <p/>
 *
 * @author Subhashni Balakrishnan
 * @since 3.0
 */
public class ReactiveStringN1qlBasedQuery extends ReactiveAbstractN1qlBasedQuery {

    private final StringBasedN1qlQueryParser queryParser;
    private final SpelExpressionParser parser;
    private final QueryMethodEvaluationContextProvider evaluationContextProvider;

    protected String getTypeField() {
        return getCouchbaseOperations().getConverter().getTypeKey();
    }

    protected Class<?> getTypeValue() {
        return getQueryMethod().getEntityInformation().getJavaType();
    }

    public ReactiveStringN1qlBasedQuery(String statement,
                                        CouchbaseQueryMethod queryMethod,
                                        CouchbaseOperations couchbaseOperations,
                                        SpelExpressionParser spelParser,
                                        QueryMethodEvaluationContextProvider evaluationContextProvider) {
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
        return x(queryParser.doParse(parser, evaluationContext, false));
    }

}
