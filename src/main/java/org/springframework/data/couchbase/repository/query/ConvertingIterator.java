/*
 * Copyright 2012-2017 the original author or authors
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

package org.springframework.data.couchbase.repository.query;

import java.util.Iterator;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;

/**
 * An {@link Iterator Iterator&lt;Object&gt;} that {@link CouchbaseConverter#convertForWriteIfNeeded(Object) converts}
 * values to their stored Class if warranted.
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 */
public class ConvertingIterator implements Iterator<Object> {
  private final Iterator<Object> delegate;
  private final CouchbaseConverter converter;

  public ConvertingIterator(Iterator<Object> delegate, CouchbaseConverter converter) {
    this.delegate = delegate;
    this.converter = converter;
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public void remove() {
    delegate.remove();
  }

  @Override
  public Object next() {
    Object next = delegate.next();
    return converter.convertForWriteIfNeeded(next);
  }
}
