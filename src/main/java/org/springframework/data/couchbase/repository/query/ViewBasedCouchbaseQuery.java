/*
 * Copyright 2013-2015 the original author or authors.
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

import java.util.List;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseQueryExecutionException;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.StringUtils;

/**
 * Execute a repository query through the View mechanism.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 */
public class ViewBasedCouchbaseQuery implements RepositoryQuery {

  private static final Logger LOG = LoggerFactory.getLogger(ViewBasedCouchbaseQuery.class);

  private final CouchbaseQueryMethod method;
  private final CouchbaseOperations operations;

  public ViewBasedCouchbaseQuery(CouchbaseQueryMethod method, CouchbaseOperations operations) {
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
    boolean isReduce = methodName.startsWith("count");
    String viewName = StringUtils.uncapitalize(methodName.replaceFirst("find|count", ""));

    ViewQuery simpleQuery = ViewQuery.from(designDoc, viewName);
    if (isReduce) {
      simpleQuery.reduce(isReduce);
      return executeReduce(simpleQuery, designDoc, viewName);
    } else {
      return execute(simpleQuery);
    }
  }

  protected Object deriveAndExecute(Object[] runtimeParams) {
    String designDoc = designDocName(method);
    String viewName = method.getViewAnnotation().viewName();
    ViewQuery baseQuery = ViewQuery.from(designDoc, viewName);
    try {
      PartTree tree = new PartTree(method.getName(), method.getEntityInformation().getJavaType());

      ViewQueryCreator creator = new ViewQueryCreator(tree,
          new ParametersParameterAccessor(method.getParameters(), runtimeParams),
          baseQuery, operations.getConverter());

      ViewQuery query = creator.createQuery();

      if (tree.isCountProjection() == Boolean.TRUE) {
        return executeReduce(query, designDoc, viewName);
      } else {
        return execute(query);
      }
    } catch (PropertyReferenceException e) {
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
    return operations.findByView(query, method.getEntityInformation().getJavaType());
  }

  protected Object executeReduce(ViewQuery query, String designDoc, String viewName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing view reduced query: " + query.toString());
    }
    ViewResult viewResult = operations.queryView(query);
    List<ViewRow> allRows = viewResult.allRows();
    JsonObject error = viewResult.error();
    if (error != null) {
      throw new CouchbaseQueryExecutionException("Error while reducing on view " + designDoc + "/" + viewName +
          ": " + error);
    }
    if (allRows == null || allRows.isEmpty()) {
      return null;
    } else{
      return allRows.get(0).value();
    }
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
