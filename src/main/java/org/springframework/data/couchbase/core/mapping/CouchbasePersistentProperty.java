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

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.context.PersistentPropertyPath;

/**
 * Represents a property part of an entity that needs to be persisted.
 *
 * @author Michael Nitschinger
 */
public interface CouchbasePersistentProperty extends PersistentProperty<CouchbasePersistentProperty> {

  /**
   * Returns the field name of the property.
   * <p/>
   * The field name can be different from the actual property name by using a custom annotation.
   */
  String getFieldName();

  /**
   * A converter that can be used to extract the {@link #getFieldName() fieldName}, eg. when one wants
   * a path from {@link PersistentPropertyPath#toDotPath(Converter)} made of field names.
   */
  Converter<? super CouchbasePersistentProperty,String> FIELD_NAME = new Converter<CouchbasePersistentProperty, String>() {
    @Override
    public String convert(CouchbasePersistentProperty source) {
      return source.getFieldName();
    }
  };


}
