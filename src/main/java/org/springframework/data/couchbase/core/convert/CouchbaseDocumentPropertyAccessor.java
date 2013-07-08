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
 * @author Michael Nitschinger
 */
public class CouchbaseDocumentPropertyAccessor extends MapAccessor {

  static MapAccessor INSTANCE = new CouchbaseDocumentPropertyAccessor();

  @Override
  public Class<?>[] getSpecificTargetClasses() {
    return new Class[] {CouchbaseDocument.class};
  }

  @Override
  public boolean canRead(EvaluationContext context, Object target, String name) {
    return true;
  }

  @Override
  public TypedValue read(EvaluationContext contect, Object target, String name) {
    Map<String, Object> source = (Map<String, Object>) target;

    Object value = source.get(name);
    return value == null ? TypedValue.NULL : new TypedValue(value);
  }
}
