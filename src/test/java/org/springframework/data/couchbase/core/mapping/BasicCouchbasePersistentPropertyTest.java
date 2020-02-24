/*
 * Copyright 2013-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.mapping;

import java.lang.reflect.Field;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the correct behavior of properties on persistable objects.
 *
 * @author Michael Nitschinger
 * @author Mark Paluch
 */
public class BasicCouchbasePersistentPropertyTest {

  /**
   * Holds the entity to test against (contains the properties).
   */
  CouchbasePersistentEntity<Beer> entity;

  /**
   * Create an instance of the demo entity.
   */
  @BeforeEach
  void beforeEach() {
    entity = new BasicCouchbasePersistentEntity<>(ClassTypeInformation.from(Beer.class));
  }

  /**
   * Verifies the name of the property without annotations.
   */
  @Test
  void usesPropertyFieldName() {
    Field field = ReflectionUtils.findField(Beer.class, "description");
    assertThat(getPropertyFor(field).getFieldName()).isEqualTo("description");
  }

  /**
   * Verifies the name of the property with custom name annotation.
   */
  @Test
  void usesAnnotatedFieldName() {
    Field field = ReflectionUtils.findField(Beer.class, "name");
    assertThat(getPropertyFor(field).getFieldName()).isEqualTo("name");
  }

  @Test
  void testSdkIdAnnotationEvaluatedAfterSpringIdAnnotationIsIgnored() {
    BasicCouchbasePersistentEntity<Beer> test = new BasicCouchbasePersistentEntity<>(
      ClassTypeInformation.from(Beer.class));
    Field springIdField = ReflectionUtils.findField(Beer.class, "springId");
    CouchbasePersistentProperty springIdProperty = getPropertyFor(springIdField);

    //here this simulates the order in which the annotations would be found
    // when "overriding" Spring @Id with SDK's @Id...
    test.addPersistentProperty(springIdProperty);

    assertThat(test.getIdProperty()).isEqualTo(springIdProperty);
  }

  /**
   * Helper method to create a property out of the field.
   *
   * @param field the field to retrieve the properties from.
   * @return the actual BasicCouchbasePersistentProperty instance.
   */
  private CouchbasePersistentProperty getPropertyFor(Field field) {

    ClassTypeInformation<?> type = ClassTypeInformation.from(field.getDeclaringClass());

    return new BasicCouchbasePersistentProperty(Property.of(type, field), entity, SimpleTypeHolder.DEFAULT,
      PropertyNameFieldNamingStrategy.INSTANCE);
  }

  /**
   * Simple POJO to test attribute properties and annotations.
   */
  public class Beer {

    @Id
    private String springId;

    @org.springframework.data.couchbase.core.mapping.Field
    String name;

    String description;

    public String getId() {
      return springId;
    }
  }

}
