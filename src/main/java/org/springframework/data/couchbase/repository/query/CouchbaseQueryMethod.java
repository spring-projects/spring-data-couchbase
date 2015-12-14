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

import org.springframework.data.couchbase.core.view.View;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;

/**
 * Represents a query method with couchbase extensions.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseQueryMethod extends QueryMethod {

  private final View viewAnnotation;

  public CouchbaseQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory factory) {
    super(method, metadata, factory);
    this.viewAnnotation = method.getAnnotation(View.class);
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
   * Returns the @View annotation if set, null otherwise.
   *
   * @return the view annotation of present.
   */
  public View getViewAnnotation() {
    return viewAnnotation;
  }
}
