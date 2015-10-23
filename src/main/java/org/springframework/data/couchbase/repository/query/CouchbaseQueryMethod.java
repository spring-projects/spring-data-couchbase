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

import java.lang.reflect.Method;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Dimensional;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.util.StringUtils;

/**
 * Represents a query method with couchbase extensions, allowing to discover
 * if View-based query or N1QL-based query must be used.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 */
public class CouchbaseQueryMethod extends QueryMethod {

  private final Method method;
  private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

  public CouchbaseQueryMethod(Method method, RepositoryMetadata metadata,
    MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext) {
    super(method, metadata);

    this.method = method;
    this.mappingContext = mappingContext;
  }

  /**
   * If the method has a @View annotation.
   *
   * @return true if it has the annotation, false otherwise.
   */
  public boolean hasViewAnnotation() {
    return getViewAnnotation() != null;
  }

  /**
   * If the method has a @View annotation with the designDocument and viewName specified.
   *
   * @return true if it has the annotation and full view specified.
   */
  public boolean hasViewSpecification() {
    return hasDesignDoc() && hasViewName();
  }

  /**
   * If the method has a @View annotation with the designDocument specified.
   *
   * @return true if it has the design document specified.
   */
  public boolean hasDesignDoc() {
    View annotation = getViewAnnotation();
    if (annotation == null) {
      return false;
    }
    return StringUtils.hasText(annotation.designDocument());
  }

  /**
   * If the method has a @View annotation with the viewName specified.
   *
   * @return true if it has the view name specified.
   */
  public boolean hasViewName() {
    View annotation = getViewAnnotation();
    if (annotation == null) {
      return false;
    }
    return StringUtils.hasText(annotation.viewName());
  }

  /**
   * Returns the @View annotation if set, null otherwise.
   *
   * @return the view annotation of present.
   */
  public View getViewAnnotation() {
    return method.getAnnotation(View.class);
  }


  /**
   * @return true if the method has a @Dimensional annotation, false otherwise.
   */
  public boolean hasDimensionalAnnotation() {
    return getDimensionalAnnotation() != null;
  }

  /**
   * @return the @Dimensional annotation if set, null otherwise.
   */
  public Dimensional getDimensionalAnnotation() {
    return AnnotationUtils.findAnnotation(method, Dimensional.class);
  }

  /**
   * If the method has a @Query annotation.
   *
   * @return true if it has the annotation, false otherwise.
   */
  public boolean hasN1qlAnnotation() {
    return getN1qlAnnotation() != null;
  }

  /**
   * Returns the @Query annotation if set, null otherwise.
   *
   * @return the n1ql annotation if present.
   */
  public Query getN1qlAnnotation() {
    return method.getAnnotation(Query.class);
  }

  /**
   * If the method has a @Query annotation with an inline Query statement inside.
   *
   * @return true if this has the annotation and N1QL inline statement, false otherwise.
   */
  public boolean hasInlineN1qlQuery() {
    return getInlineN1qlQuery() != null;
  }

  /**
   * Returns the query string declared in a {@link Query} annotation or {@literal null} if neither the annotation found
   * nor the attribute was specified.
   *
   * @return the query statement if present.
   */
  public String getInlineN1qlQuery() {
    String query = (String) AnnotationUtils.getValue(getN1qlAnnotation());
    return StringUtils.hasText(query) ? query : null;
  }
}
