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

import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;

/**
 * The Couchbase Type Mapper.
 *
 * @author Michael Nitschinger
 */
public class DefaultCouchbaseTypeMapper extends DefaultTypeMapper<CouchbaseDocument> implements CouchbaseTypeMapper {

  /**
   * The type key to use if a complex type was identified.
   */
  public static final String DEFAULT_TYPE_KEY = "_class";

  /**
   * Create a new type mapper with the type key.
   *
   * @param typeKey the typeKey to use.
   */
  public DefaultCouchbaseTypeMapper(final String typeKey) {
    super(new CouchbaseDocumentTypeAliasAccessor(typeKey));
  }

  public static final class CouchbaseDocumentTypeAliasAccessor implements TypeAliasAccessor<CouchbaseDocument> {

    private final String typeKey;

    public CouchbaseDocumentTypeAliasAccessor(final String typeKey) {
      this.typeKey = typeKey;
    }

    @Override
    public Object readAliasFrom(final CouchbaseDocument source) {
      return source.get(typeKey);
    }

    @Override
    public void writeTypeTo(final CouchbaseDocument sink, final Object alias) {
      if (typeKey != null) {
        sink.put(typeKey, alias);
      }
    }
  }

}
