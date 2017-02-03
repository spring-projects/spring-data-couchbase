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

package org.springframework.data.couchbase.core.mapping;

import com.couchbase.client.java.repository.annotation.Field;
import com.couchbase.client.java.repository.annotation.Id;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.StringUtils;

import java.util.Optional;

/**
 * Implements annotated property representations of a given {@link com.couchbase.client.java.repository.annotation.Field} instance.
 * <p/>
 * <p>This object is used to gather information out of properties on objects that need to be persisted. For example, it
 * supports overriding of the actual property name by providing custom annotations.</p>
 *
 * @author Michael Nitschinger
 * @author Mark Paluch
 */
public class BasicCouchbasePersistentProperty
        extends AnnotationBasedPersistentProperty<CouchbasePersistentProperty>
        implements CouchbasePersistentProperty {

  private final FieldNamingStrategy fieldNamingStrategy;

  /**
   * Create a new instance of the BasicCouchbasePersistentProperty class.
   *
   * @param property         the PropertyDescriptor.
   * @param owner            the original owner of the property.
   * @param simpleTypeHolder the type holder.
   */
  public BasicCouchbasePersistentProperty(Property property,
                                          final CouchbasePersistentEntity<?> owner, final SimpleTypeHolder simpleTypeHolder,
                                          final FieldNamingStrategy fieldNamingStrategy) {
    super(property, owner, simpleTypeHolder);
    this.fieldNamingStrategy = fieldNamingStrategy == null ? PropertyNameFieldNamingStrategy.INSTANCE
            : fieldNamingStrategy;
  }

  /**
   * Creates a new Association.
   */
  @Override
  protected Association<CouchbasePersistentProperty> createAssociation() {
    return new Association<CouchbasePersistentProperty>(this, null);
  }

  /**
   * Returns the field name of the property.
   * <p/>
   * The field name can be different from the actual property name by using a
   * custom annotation.
   */
  @Override
  public String getFieldName() {
    Optional<Field> annotation = findAnnotation(com.couchbase.client.java.repository.annotation.Field.class);

    return annotation.map(Field::value).filter(StringUtils::hasText).orElseGet(() -> {

      String fieldName = fieldNamingStrategy.getFieldName(this);

      if (!StringUtils.hasText(fieldName)) {
        throw new MappingException(String.format("Invalid (null or empty) field name returned for property %s by %s!",
                this, fieldNamingStrategy.getClass()));
      }

      return fieldName;
    });

  }

  // DATACOUCH-145: allows SDK's @Id annotation to be used
  @Override
  public boolean isIdProperty() {
    return isAnnotationPresent(Id.class) || super.isIdProperty();
  }
}
