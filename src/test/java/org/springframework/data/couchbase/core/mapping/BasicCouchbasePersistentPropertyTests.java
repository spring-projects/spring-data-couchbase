/*
 * Copyright 2013-2017 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

/**
 * Verifies the correct behavior of properties on persistable objects.
 *
 * @author Michael Nitschinger
 * @author Mark Paluch
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

  @Test
  public void testPrefersSpringIdAnnotation() {
    BasicCouchbasePersistentEntity<Beer> test = new BasicCouchbasePersistentEntity<Beer>(
            ClassTypeInformation.from(Beer.class));

    Field sdkIdField = ReflectionUtils.findField(Beer.class, "sdkId");
    CouchbasePersistentProperty sdkIdProperty = getPropertyFor(sdkIdField);
    Field springIdField = ReflectionUtils.findField(Beer.class, "springId");
    CouchbasePersistentProperty springIdProperty = getPropertyFor(springIdField);
    test.addPersistentProperty(sdkIdProperty);
    test.addPersistentProperty(springIdProperty);

    assertEquals("sdkId", sdkIdProperty.getFieldName());
    assertEquals("springId", springIdProperty.getFieldName());

    assertTrue(sdkIdProperty.isIdProperty());
    assertTrue(springIdProperty.isIdProperty());

    Optional<CouchbasePersistentProperty> property = test.getIdProperty();
    assertTrue(property.isPresent());
    property.ifPresent(actual -> {
      assertEquals(springIdProperty, actual);
    });
  }

  @Test
  public void testAcceptsSdkIdAnnotation() {
    BasicCouchbasePersistentEntity<SdkIdentified> test = new BasicCouchbasePersistentEntity<SdkIdentified>(
            ClassTypeInformation.from(SdkIdentified.class));
    Field id = ReflectionUtils.findField(SdkIdentified.class, "id");
    CouchbasePersistentProperty idProperty = getPropertyFor(id);
    test.addPersistentProperty(idProperty);

    Optional<CouchbasePersistentProperty> property = test.getIdProperty();
    assertTrue(property.isPresent());
    property.ifPresent(actual -> {
      assertEquals(idProperty, actual);
    });
  }

  @Test
  public void testSdkIdAnnotationEvaluatedAfterSpringIdAnnotationIsIgnored() {
    BasicCouchbasePersistentEntity<Beer> test = new BasicCouchbasePersistentEntity<Beer>(
            ClassTypeInformation.from(Beer.class));
    Field sdkIdField = ReflectionUtils.findField(Beer.class, "sdkId");
    CouchbasePersistentProperty sdkIdProperty = getPropertyFor(sdkIdField);
    Field springIdField = ReflectionUtils.findField(Beer.class, "springId");
    CouchbasePersistentProperty springIdProperty = getPropertyFor(springIdField);

    //here this simulates the order in which the annotations would be found
    // when "overriding" Spring @Id with SDK's @Id...
    test.addPersistentProperty(springIdProperty);

    Optional<CouchbasePersistentProperty> property = test.getIdProperty();
    assertTrue(property.isPresent());
    property.ifPresent(actual -> {
      assertEquals(springIdProperty, actual);
    });

    test.addPersistentProperty(sdkIdProperty);

    property = test.getIdProperty();
    assertTrue(property.isPresent());
    property.ifPresent(actual -> {
      assertEquals(springIdProperty, actual);
    });
  }

  /**
   * Helper method to create a property out of the field.
   *
   * @param field the field to retrieve the properties from.
   * @return the actual BasicCouchbasePersistentProperty instance.
   */
  private CouchbasePersistentProperty getPropertyFor(Field field) {
    return new BasicCouchbasePersistentProperty(Property.of(field), entity, new SimpleTypeHolder(),
            PropertyNameFieldNamingStrategy.INSTANCE);
  }

  /**
   * Simple POJO to test attribute properties and annotations.
   */
  public class Beer {

    @com.couchbase.client.java.repository.annotation.Id
    private String sdkId;

    @Id
    private String springId;

    @com.couchbase.client.java.repository.annotation.Field("foobar")
    String name;

    String description;

    public String getId() {
      return springId;
    }
  }

  /**
   * Simple POJO to test that a single ID property from the SDK is taken into account.
   */
  public class SdkIdentified {
    @com.couchbase.client.java.repository.annotation.Id
    private String id;

    String value;
  }
}
