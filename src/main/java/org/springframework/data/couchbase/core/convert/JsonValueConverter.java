/*
 * Copyright 2022-2024 the original author or authors
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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.mapping.PersistentProperty;

/**
 * Converter for non-Enum types that have @JsonValue and possibly an @JsonCreator annotated methods.
 * 
 * @author Michael Reiche
 */
public class JsonValueConverter
		implements PropertyValueConverter<Object, Object, ValueConversionContext<? extends PersistentProperty<?>>> {

	static protected final Map<Class<?>, Method> valueMethodCache = new ConcurrentHashMap<>();
	static protected final Map<Class<?>, Constructor<?>> creatorMethodCache = new ConcurrentHashMap<>();
	static private final ConverterHasNoConversion CONVERTER_HAS_NO_CONVERSION = new ConverterHasNoConversion();

	@Override
	public Object read(Object value, ValueConversionContext<? extends PersistentProperty<?>> context) {
		Class<?> type = context.getProperty().getType();

		// if there was a @JsonCreator method, use it
		if (getJsonCreatorMethod(type) != null) {
			try {
				return getJsonCreatorMethod(type).newInstance(value);
			} catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
				throw new RuntimeException(e);
			}
		}

		// fall-through in MappingCouchbaseConverter.readValue(), maybe there is an @PersistenceCreator that takes the arg.
		throw CONVERTER_HAS_NO_CONVERSION;
	}

	@Override
	public Object write(Object value, ValueConversionContext<? extends PersistentProperty<?>> context) {
		Class<?> type = value.getClass();
		try {
			return getJsonValueMethod(type).invoke(value);
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	private Method getJsonValueMethod(Class<?> type) {
		return valueMethodCache.get(type);
	}

	private Constructor<?> getJsonCreatorMethod(Class<?> type) {
		return creatorMethodCache.get(type);
	}

}
