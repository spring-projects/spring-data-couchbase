/*
 * Copyright 2022 the original author or authors
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
package org.springframework.data.couchbase.core.convert;

import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * {@link ValueConversionContext} that allows to delegate read/write to an underlying {@link CouchbaseConverter}.
 *
 * @author Michael Reiche
 * @since 5.0
 */
public class CouchbaseConversionContext implements ValueConversionContext<CouchbasePersistentProperty> {

	private final CouchbasePersistentProperty persistentProperty;
	private final MappingCouchbaseConverter couchbaseConverter;
	private final ConvertingPropertyAccessor propertyAccessor;

	public CouchbaseConversionContext(CouchbasePersistentProperty persistentProperty,
			MappingCouchbaseConverter couchbaseConverter, ConvertingPropertyAccessor accessor) {

		this.persistentProperty = persistentProperty;
		this.couchbaseConverter = couchbaseConverter;
		this.propertyAccessor = accessor;
	}

	@Override
	public CouchbasePersistentProperty getProperty() {
		return persistentProperty;
	}

	@Override
	public <T> T write(@Nullable Object value, TypeInformation<T> target) {
		return (T) ValueConversionContext.super.write(value, target);
	}

	@Override
	public <T> T read(@Nullable Object value, TypeInformation<T> target) {
		return ValueConversionContext.super.read(value, target);
	}

	public MappingCouchbaseConverter getConverter() {
		return couchbaseConverter;
	}

	public ConvertingPropertyAccessor getAccessor() {
		return propertyAccessor;
	}
}
