/*
 * Copyright 2012-2015 the original author or authors
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


import static java.util.stream.Collectors.toSet;

import java.util.stream.Stream;

import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;

import org.springframework.data.mapping.model.SimpleTypeHolder;

public abstract class CouchbaseSimpleTypes {

  public static final SimpleTypeHolder JSON_TYPES = new SimpleTypeHolder(
    Stream.of(RawJsonDocument.class,
              JsonArray.class,
              Number.class).collect(toSet()),
    true);

  public static final SimpleTypeHolder DOCUMENT_TYPES = new SimpleTypeHolder(
    Stream.of(CouchbaseDocument.class,
              CouchbaseList.class).collect(toSet()),
    true);

  private CouchbaseSimpleTypes() {
  }

}
