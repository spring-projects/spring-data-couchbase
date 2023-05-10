/*
 * Copyright 2013-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.Field;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;
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
	@BeforeEach
	void beforeEach() {
		entity = new BasicCouchbasePersistentEntity<>(TypeInformation.of(Beer.class));
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
			TypeInformation.of(Beer.class));
		Field springIdField = ReflectionUtils.findField(Beer.class, "springId");
		CouchbasePersistentProperty springIdProperty = getPropertyFor(springIdField);

		// here this simulates the order in which the annotations would be found
		// when "overriding" Spring @Id with SDK's @Id...
		test.addPersistentProperty(springIdProperty);

		assertThat(test.getIdProperty()).isEqualTo(springIdProperty);
	}

	@Test
	void testAnnotationIdFieldOnly() { // only has @springId
		class TestIdField {
			@org.springframework.data.couchbase.core.mapping.Field String name;
			String description;
			@Id private String springId;
		}
		BasicCouchbasePersistentEntity<TestIdField> test = new BasicCouchbasePersistentEntity<>(
			TypeInformation.of(TestIdField.class));
		Field springIdField = ReflectionUtils.findField(TestIdField.class, "springId");
		CouchbasePersistentProperty springIdProperty = getPropertyFor(springIdField);
		test.addPersistentProperty(springIdProperty);
		assertThat(test.getIdProperty()).isEqualTo(springIdProperty);
	}

	@Test
	void testIdFieldOnly() { // only has id
		class TestIdField {
			@org.springframework.data.couchbase.core.mapping.Field String name;
			String description;
			private String id;
		}
		Field idField = ReflectionUtils.findField(TestIdField.class, "id");
		CouchbasePersistentProperty idProperty = getPropertyFor(idField);
		BasicCouchbasePersistentEntity<TestIdField> test = new BasicCouchbasePersistentEntity<>(
			TypeInformation.of(TestIdField.class));
		test.addPersistentProperty(idProperty);
		assertThat(test.getIdProperty()).isEqualTo(idProperty);
	}

	@Test
	void testIdFieldAndAnnotationIdField() { // has @springId and id
		class TestIdField {
			@org.springframework.data.couchbase.core.mapping.Field String name;
			String description;
			@Id private String springId;
			private String id;
		}
		BasicCouchbasePersistentEntity<TestIdField> test = new BasicCouchbasePersistentEntity<>(
			TypeInformation.of(TestIdField.class));
		Field springIdField = ReflectionUtils.findField(TestIdField.class, "springId");
		Field idField = ReflectionUtils.findField(TestIdField.class, "id");
		CouchbasePersistentProperty idProperty = getPropertyFor(idField);
		CouchbasePersistentProperty springIdProperty = getPropertyFor(springIdField);
		// here this simulates the order in which the annotations would be found
		// when "overriding" Spring @Id with SDK's @Id...
		test.addPersistentProperty(idProperty);
		// replace id with springId
		test.addPersistentProperty(springIdProperty);
		assertThat(test.getIdProperty()).isEqualTo(springIdProperty);
	}

	@Test
	void testTwoAnnotationIdFields() { // has @Id springId and @Id id
		class TestIdField {
			@org.springframework.data.couchbase.core.mapping.Field String name;
			String description;
			@Id private String springId;
			@Id private String id;
		}
		Field springIdField = ReflectionUtils.findField(TestIdField.class, "springId");
		Field idField = ReflectionUtils.findField(TestIdField.class, "id");
		CouchbasePersistentProperty idProperty = getPropertyFor(idField);
		CouchbasePersistentProperty springIdProperty = getPropertyFor(springIdField);
		BasicCouchbasePersistentEntity<TestIdField> test = new BasicCouchbasePersistentEntity<>(
			TypeInformation.of(TestIdField.class));
		test.addPersistentProperty(springIdProperty);
		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> {
			test.addPersistentProperty(idProperty);
		});
	}

	@Test
	void testTwoIdFields() { // has @Field("id") springId and id
		class TestIdField {
			@org.springframework.data.couchbase.core.mapping.Field String name;
			String description;
			@org.springframework.data.couchbase.core.mapping.Field("id") private String springId;
			private String id;
		}
		Field springIdField = ReflectionUtils.findField(TestIdField.class, "springId");
		Field idField = ReflectionUtils.findField(TestIdField.class, "id");
		CouchbasePersistentProperty idProperty = getPropertyFor(idField);
		CouchbasePersistentProperty springIdProperty = getPropertyFor(springIdField);
		BasicCouchbasePersistentEntity<TestIdField> test = new BasicCouchbasePersistentEntity<>(
			TypeInformation.of(TestIdField.class));
		test.addPersistentProperty(springIdProperty);
		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> {
			test.addPersistentProperty(idProperty);
		});
	}

	/**
	 * Helper method to create a property out of the field.
	 *
	 * @param field the field to retrieve the properties from.
	 * @return the actual BasicCouchbasePersistentProperty instance.
	 */
	private CouchbasePersistentProperty getPropertyFor(Field field) {

		TypeInformation<?> type = TypeInformation.of(field.getDeclaringClass());

		return new BasicCouchbasePersistentProperty(Property.of(type, field), entity, SimpleTypeHolder.DEFAULT,
				PropertyNameFieldNamingStrategy.INSTANCE);
	}

	/**
	 * Simple POJO to test attribute properties and annotations.
	 */
	public class Beer {

		@org.springframework.data.couchbase.core.mapping.Field String name;
		String description;
		@Id private String springId;

		public String getId() {
			return springId;
		}
	}

}
