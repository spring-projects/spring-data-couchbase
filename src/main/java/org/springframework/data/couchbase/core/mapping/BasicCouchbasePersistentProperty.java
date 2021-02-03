/*
 * Copyright 2012-2021 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.mapping;

import java.util.Locale;

import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.Lazy;
import org.springframework.util.StringUtils;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Implements annotated property representations of a given {@link Field} instance.
 * <p/>
 * <p>
 * This object is used to gather information out of properties on objects that need to be persisted. For example, it
 * supports overriding of the actual property name by providing custom annotations.
 * </p>
 *
 * @author Michael Nitschinger
 * @author Mark Paluch
 */
public class BasicCouchbasePersistentProperty extends AnnotationBasedPersistentProperty<CouchbasePersistentProperty>
		implements CouchbasePersistentProperty {

	private final FieldNamingStrategy fieldNamingStrategy;

	/**
	 * Create a new instance of the BasicCouchbasePersistentProperty class.
	 *
	 * @param property the PropertyDescriptor.
	 * @param owner the original owner of the property.
	 * @param simpleTypeHolder the type holder.
	 */
	public BasicCouchbasePersistentProperty(Property property, final CouchbasePersistentEntity<?> owner,
			final SimpleTypeHolder simpleTypeHolder, final FieldNamingStrategy fieldNamingStrategy) {
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
	 * The field name can be different from the actual property name by using a custom annotation.
	 */
	@Override
	public String getFieldName() {
		Field annotationField = getField().getAnnotation(Field.class);

		if (annotationField != null && StringUtils.hasText(annotationField.value())) {
			return annotationField.value();
		}
		JsonProperty annotation = getField().getAnnotation(JsonProperty.class);

		if (annotation != null && StringUtils.hasText(annotation.value())) {
			return annotation.value();
		}

		String fieldName = fieldNamingStrategy.getFieldName(this);

		if (!StringUtils.hasText(fieldName)) {
			throw new MappingException(String.format("Invalid (null or empty) field name returned for property %s by %s!",
					this, fieldNamingStrategy.getClass()));
		}

		return fieldName;
	}

	// DATACOUCH-145: allows SDK's @Id annotation to be used
	@Override
	public boolean isIdProperty() {
		if (super.isIdProperty()) {
			return true;
		}
		// is field named "id"
		if (getField() != null && this.getFieldName().toLowerCase(Locale.ROOT).equals("id")) {
			return true;
		}
		return false;
	}

	public Boolean isExpirationProperty() {
		return isExpiration.get();
	}

	private final Lazy<Boolean> isExpiration = Lazy.of(() -> {
		return this.isAnnotationPresent(Expiration.class);
	});

}
