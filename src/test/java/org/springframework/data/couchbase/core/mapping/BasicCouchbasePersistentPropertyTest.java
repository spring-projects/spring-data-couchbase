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

package org.springframework.data.couchbase.core.mapping;

import java.lang.reflect.Field;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.BasicCouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.BasicCouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

/**
 * Verifies the correct behavior of properties on persistable objects.
 */
public class BasicCouchbasePersistentPropertyTest {

  /**
   * Holds the entity to test against (contains the properties).
   */
  CouchbasePersistentEntity<Beer> entity;

  /**
   * Create an instance of the demo entity.
   */
  @Before
  public void setUp() {
    entity = new BasicCouchbasePersistentEntity<Beer>(
      ClassTypeInformation.from(Beer.class));
  }

  /**
   * Verifies the name of the property without annotations.
   */
  @Test
  public void usesPropertyFieldName() {
    Field field = ReflectionUtils.findField(Beer.class, "description");
    assertEquals("description", getPropertyFor(field).getFieldName());
  }

  /**
   * Verifies the name of the property with custom name annotation.
   */
  @Test
  public void usesAnnotatedFieldName() {
    Field field = ReflectionUtils.findField(Beer.class, "name");
    assertEquals("foobar", getPropertyFor(field).getFieldName());
  }

  /**
   * Helper method to create a property out of the field.
   *
   * @param field the field to retrieve the properties from.
   * @return the actual BasicCouchbasePersistentProperty instance.
   */
  private CouchbasePersistentProperty getPropertyFor(Field field) {
    return new BasicCouchbasePersistentProperty(field, null, entity,
      new SimpleTypeHolder());
  }

  /**
   * Simple POJO to test attribute properties and annotations.
   */
  public class Beer {

    @Id
    private String id;

    @org.springframework.data.couchbase.core.mapping.Field("foobar")
    String name;

    String description;

  }
}
