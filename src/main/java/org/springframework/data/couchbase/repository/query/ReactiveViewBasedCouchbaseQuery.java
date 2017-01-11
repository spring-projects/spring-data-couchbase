/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.query;

import com.couchbase.client.java.view.AsyncViewRow;
import com.couchbase.client.java.view.ViewQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.couchbase.core.CouchbaseQueryExecutionException;
import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import rx.Observable;

/**
 * Execute a reactive repository query through the View mechanism.
 *
 * @author Subhashni Balakrishnan
 * @since 3.0
 */
public class ReactiveViewBasedCouchbaseQuery implements RepositoryQuery {

    private static final Logger LOG = LoggerFactory.getLogger(ReactiveViewBasedCouchbaseQuery.class);

    private final CouchbaseQueryMethod method;
    private final RxJavaCouchbaseOperations operations;

    public ReactiveViewBasedCouchbaseQuery(CouchbaseQueryMethod method, RxJavaCouchbaseOperations operations) {
        this.method = method;
        this.operations = operations;
    }

    @Override
    public Object execute(Object[] runtimeParams) {
        if (method.hasViewName()) { //only allow derivation on @View explicitly defining a viewName
            return deriveAndExecute(runtimeParams);
        } else {
            return guessViewAndExecute();
        }
    }

    protected Object guessViewAndExecute() {
        String designDoc = designDocName(method);
        String methodName = method.getName();
        boolean isExplicitReduce = method.hasViewAnnotation() && method.getViewAnnotation().reduce();
        boolean isReduce = methodName.startsWith("count") || isExplicitReduce;
        String viewName = StringUtils.uncapitalize(methodName.replaceFirst("find|count", ""));

        ViewQuery simpleQuery = ViewQuery.from(designDoc, viewName)
                .stale(operations.getDefaultConsistency().viewConsistency());
        if (isReduce) {
            simpleQuery.reduce();
            return executeReduce(simpleQuery, designDoc, viewName);
        } else {
            return execute(simpleQuery);
        }
    }

    protected Object deriveAndExecute(Object[] runtimeParams) {
        String designDoc = designDocName(method);
        String viewName = method.getViewAnnotation().viewName();

        //prepare a ViewQuery to be used as a base for the ViewQueryCreator
        ViewQuery baseQuery = ViewQuery.from(designDoc, viewName)
                .stale(operations.getDefaultConsistency().viewConsistency());

        try {
            PartTree tree = new PartTree(method.getName(), method.getEntityInformation().getJavaType());

            //use a ViewQueryCreator to complete the base query
            ViewQueryCreator creator = new ViewQueryCreator(tree, new ReactiveCouchbaseParameterAccessor(method, runtimeParams),
                    method.getViewAnnotation(), baseQuery, operations.getConverter());
            ViewQueryCreator.DerivedViewQuery result = creator.createQuery();

            if (result.isReduce) {
                return executeReduce(result.builtQuery, designDoc, viewName);
            } else {
                //otherwise just execute the query
                return execute(result.builtQuery);
            }
        } catch (PropertyReferenceException e) {
      /*
        For views, not including an attribute name in the method will result in returning
        the whole set of results from the view.
        This is detected by looking for PropertyReferenceExceptions that seem to complain
        about a missing property that corresponds to the method name
     */
            if (e.getPropertyName().equals(method.getName())) {
                return execute(baseQuery);
            }
            throw e;
        }
    }

    protected Object execute(ViewQuery query) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing view query: " + query.toString());
        }
        return ReactiveWrapperConverters.toWrapper(operations.findByView(query, method.getEntityInformation().getJavaType()),
                Flux.class);
    }

    protected Object executeReduce(ViewQuery query, String designDoc, String viewName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing view reduced query: " + query.toString());
        }
        return ReactiveWrapperConverters.toWrapper(operations.queryView(query)
                .flatMap(asyncViewResult -> asyncViewResult.error()
                        .flatMap(error -> Observable.error(new CouchbaseQueryExecutionException("Unable to execute reducing view "
                                + viewName +" in design document " +  designDoc +
                                "due to error:" + error.toString())))
                        .switchIfEmpty(asyncViewResult.rows()))
                .map(row -> {
                    AsyncViewRow asyncViewRow = (AsyncViewRow) row;
                    return asyncViewRow.value();
                }).take(1), Flux.class);
    }

    @Override
    public QueryMethod getQueryMethod() {
        return method;
    }

    /**
     * Returns the best-guess design document name.
     *
     * @return the design document name.
     */
    private static String designDocName(CouchbaseQueryMethod method) {
        if (method.hasViewSpecification()) {
            return method.getViewAnnotation().designDocument();
        } else if (method.hasViewAnnotation()) {
            return StringUtils.uncapitalize(method.getEntityInformation().getJavaType().getSimpleName());
        } else {
            throw new IllegalStateException("View-based query should only happen on a method with @View annotation");
        }
    }

}
