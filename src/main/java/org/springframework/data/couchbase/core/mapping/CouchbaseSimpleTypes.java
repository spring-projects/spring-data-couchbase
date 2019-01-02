/*
 * Copyright 2012-2019 the original author or authors
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

package org.springframework.data.couchbase.core.mapping;


import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;

import org.springframework.data.mapping.model.SimpleTypeHolder;

public abstract class CouchbaseSimpleTypes {

  static {
    Set<Class<?>> simpleTypes = new HashSet<Class<?>>();
    simpleTypes.add(RawJsonDocument.class);
    simpleTypes.add(JsonArray.class);
    simpleTypes.add(Number.class);
    COUCHBASE_SIMPLE_TYPES = Collections.unmodifiableSet(simpleTypes);
  }

  private static final Set<Class<?>> COUCHBASE_SIMPLE_TYPES;
  public static final SimpleTypeHolder HOLDER = new SimpleTypeHolder(COUCHBASE_SIMPLE_TYPES, true);

  private CouchbaseSimpleTypes() {
  }

}
