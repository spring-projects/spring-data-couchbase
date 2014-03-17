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

package org.springframework.data.couchbase.core.mapping.event;

import org.springframework.context.ApplicationEvent;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;

/**
 * A mapping event.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseMappingEvent<T> extends ApplicationEvent {

  private final CouchbaseDocument document;

  public CouchbaseMappingEvent(T source, CouchbaseDocument document) {
    super(source);
    this.document = document;
  }

  public CouchbaseDocument getDocument() {
    return document;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T getSource() {
    return (T) super.getSource();
  }
}
