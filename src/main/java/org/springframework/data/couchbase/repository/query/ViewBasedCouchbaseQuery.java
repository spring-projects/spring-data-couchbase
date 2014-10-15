/*
 * Copyright 2013, 2014 the original author or authors.
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

import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.util.StringUtils;

import com.couchbase.client.java.view.ViewQuery;

/**
 * Execute a repository query through the View mechanism.
 *
 * @author Michael Nitschinger
 */
public class ViewBasedCouchbaseQuery implements RepositoryQuery {

  private static final Logger LOGGER = LoggerFactory.getLogger(ViewBasedCouchbaseQuery.class);
  private static final Field DESIGN_FIELD;
  private static final Field VIEW_FIELD;

  static {
    Field design = null;
    Field view = null;
    try {
      design = ViewQuery.class.getDeclaredField("design");
      design.setAccessible(true);
      view = ViewQuery.class.getDeclaredField("view");
      view.setAccessible(true);
    } catch (final Exception e) {
      LOGGER.error("Cannot find ViewQuery fields by reflection", e);
    } finally {
      DESIGN_FIELD = design;
      VIEW_FIELD = view;
    }
  }

  private final CouchbaseQueryMethod method;
  private final CouchbaseOperations operations;

  public ViewBasedCouchbaseQuery(CouchbaseQueryMethod method, CouchbaseOperations operations) {
    this.method = method;
    this.operations = operations;
  }

  @Override
  public Object execute(Object[] runtimeParams) {
    ViewQuery query = null;
    for (Object param : runtimeParams) {
      if (param instanceof ViewQuery) {
        query = (ViewQuery) param;
      } else {
        throw new IllegalStateException("Unknown query param: " + param);
      }
    }

    if (query == null) {
      query = ViewQuery.from(designDocName(), viewName());
    }
    query.reduce(false);

    try {
      DESIGN_FIELD.set(query, designDocName());
      VIEW_FIELD.set(query, viewName());
    } catch (final Exception e) {
      LOGGER.error("cannot Set design document or view on query");
    }

    return operations.findByView(query, method.getEntityInformation().getJavaType());
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
  private String designDocName() {
    if (method.hasViewAnnotation()) {
      return method.getViewAnnotation().designDocument();
    } else {
      return StringUtils.uncapitalize(method.getEntityInformation().getJavaType().getSimpleName());
    }
  }

  /**
   * Returns the best-guess view name.
   *
   * @return the view name.
   */
  private String viewName() {
    if (method.hasViewAnnotation()) {
      return method.getViewAnnotation().viewName();
    } else {
      return StringUtils.uncapitalize(method.getName().replaceFirst("find", ""));
    }
  }

}
