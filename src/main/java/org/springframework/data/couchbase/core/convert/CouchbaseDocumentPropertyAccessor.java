/*
 * Copyright 2013 the original author or authors.
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

package org.springframework.data.couchbase.core.convert;

import org.springframework.context.expression.MapAccessor;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.TypedValue;

import java.util.Map;

/**
 * A property accessor for document properties.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseDocumentPropertyAccessor extends MapAccessor {

  /**
   * Contains the static instance of thi accessor.
   */
  static final MapAccessor INSTANCE = new CouchbaseDocumentPropertyAccessor();

  /**
   * Returns the target classes of the properties.
   *
   * @return
   */
  @Override
  public Class<?>[] getSpecificTargetClasses() {
    return new Class[] {CouchbaseDocument.class};
  }

  /**
   * It can always read from those properties.
   *
   * @param context the evaluation context.
   * @param target the target object.
   * @param name the name of the property.
   * @return always true.
   */
  @Override
  public boolean canRead(final EvaluationContext context, final Object target, final String name) {
    return true;
  }

  /**
   * Read the value from the property.
   *
   * @param context the evaluation context.
   * @param target the target object.
   * @param name the name of the property.
   * @return the typed value of the content to be read.
   */
  @Override
  public TypedValue read(final EvaluationContext context, final Object target, final String name) {
    Map<String, Object> source = (Map<String, Object>) target;

    Object value = source.get(name);
    return value == null ? TypedValue.NULL : new TypedValue(value);
  }
}
