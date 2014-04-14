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

package org.springframework.data.couchbase.core.mapping;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

/**
 * Verifies the correct behavior of properties on persistable objects.
 *
 * @author Michael Nitschinger
 */
public class BasicCouchbasePersistentPropertyTests {

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
      new SimpleTypeHolder(), FallbackFieldNamingStrategy.INSTANCE);
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

    public String getId() {
      return id;
    }
  }
}
