/**
 * Copyright (C) 2009-2012 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.spring.core.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.StringUtils;

/**
 * Implements annotated property representations of a given Field instance.
 *
 * This object is used to gather information out of properties on objects
 * that need to be persisted. For example, it supports overriding of the
 * actual property name by providing custom annotations.
 */
public class BasicCouchbasePersistentProperty
  extends AnnotationBasedPersistentProperty<CouchbasePersistentProperty>
  implements CouchbasePersistentProperty {

  /**
   * Create a new instance of the BasicCouchbasePersistentProperty class.
   *
   * @param field the field of the original reflection.
   * @param propertyDescriptor the PropertyDescriptor.
   * @param owner the original owner of the property.
   * @param simpleTypeHolder the type holder.
   */
  public BasicCouchbasePersistentProperty(Field field,
    PropertyDescriptor propertyDescriptor, CouchbasePersistentEntity<?> owner,
    SimpleTypeHolder simpleTypeHolder) {
    super(field, propertyDescriptor, owner, simpleTypeHolder);
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
   *
   * The field name can be different from the actual property name by using a
   * custom annotation.
   */
  @Override
  public String getFieldName() {
    com.couchbase.spring.core.mapping.Field annotation = getField().
      getAnnotation(com.couchbase.spring.core.mapping.Field.class);

    return annotation != null && StringUtils.hasText(annotation.value())
      ? annotation.value() : field.getName();
  }

}
