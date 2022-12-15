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

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.couchbase.client.core.encryption.CryptoManager;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Reading Converter factory for Enums. This differs from the one provided in org.springframework.core.convert.support
 * by getting the result from the jackson objectmapper (which will process @JsonValue annotations) This is registered in
 * {@link org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration#customConversions(CryptoManager)}.
 * This will take precedence over {@link org.springframework.core.convert.support.StringToEnumConverterFactory}
 * 
 * @author Michael Reiche
 */
@ReadingConverter
public class StringToEnumConverterFactory implements ConverterFactory<String, Enum> {

	private final ObjectMapper objectMapper;

	public StringToEnumConverterFactory(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public <T extends Enum> Converter<String, T> getConverter(Class<T> targetType) {
		return new StringToEnum(getEnumType(targetType), objectMapper);
	}

	public static Class<?> getEnumType(Class<?> targetType) {
		Class<?> enumType = targetType;
		while (enumType != null && !enumType.isEnum()) {
			enumType = enumType.getSuperclass();
		}
		Assert.notNull(enumType, () -> "The target type " + targetType.getName() + " does not refer to an enum");
		return enumType;
	}

	private static class StringToEnum<T extends Enum> implements Converter<String, T> {

		private final Class<T> enumType;
		private final ObjectMapper objectMapper;

		StringToEnum(Class<T> enumType, ObjectMapper objectMapper) {
			this.enumType = enumType;
			this.objectMapper = objectMapper;
		}

		@Override
		@Nullable
		public T convert(String source) {
			if (source == null) {
				return null;
			}
			return objectMapper.convertValue(source, enumType);
		}
	}
}
