/*
 * Copyright 2021-2023 the original author or authors
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

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.util.Base64Utils;

import com.couchbase.client.core.encryption.CryptoManager;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Out of the box conversions for Other types.
 *
 * @author Michael Reiche
 */
public final class OtherConverters {

	private OtherConverters() {}

	/**
	 * Returns all converters by this class that can be registered.
	 *
	 * @return the list of converters to register.
	 */
	public static Collection<Converter<?, ?>> getConvertersToRegister() {
		List<Converter<?, ?>> converters = new ArrayList<Converter<?, ?>>();

		converters.add(UuidToString.INSTANCE);
		converters.add(StringToUuid.INSTANCE);
		converters.add(BigIntegerToString.INSTANCE);
		converters.add(StringToBigInteger.INSTANCE);
		converters.add(BigDecimalToString.INSTANCE);
		converters.add(StringToBigDecimal.INSTANCE);
		converters.add(ByteArrayToString.INSTANCE);
		converters.add(StringToByteArray.INSTANCE);
		converters.add(CharArrayToString.INSTANCE);
		converters.add(StringToCharArray.INSTANCE);
		converters.add(ClassToString.INSTANCE);
		converters.add(StringToClass.INSTANCE);
		// EnumToObject, IntegerToEnumConverterFactory and StringToEnumConverterFactory are
		// registered in
		// {@link org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration#customConversions(
		// CryptoManager)} as they require an ObjectMapper
		return converters;
	}

	@WritingConverter
	public enum UuidToString implements Converter<UUID, String> {
		INSTANCE;

		@Override
		public String convert(UUID source) {
			return source == null ? null : source.toString();
		}
	}

	@ReadingConverter
	public enum StringToUuid implements Converter<String, UUID> {
		INSTANCE;

		@Override
		public UUID convert(String source) {
			return source == null ? null : UUID.fromString(source);
		}
	}

	@WritingConverter
	public enum BigIntegerToString implements Converter<BigInteger, String> {
		INSTANCE;

		@Override
		public String convert(BigInteger source) {
			return source == null ? null : source.toString();
		}
	}

	@ReadingConverter
	public enum StringToBigInteger implements Converter<String, BigInteger> {
		INSTANCE;

		@Override
		public BigInteger convert(String source) {
			return source == null ? null : new BigInteger(source);
		}
	}

	@WritingConverter
	public enum BigDecimalToString implements Converter<BigDecimal, String> {
		INSTANCE;

		@Override
		public String convert(BigDecimal source) {
			return source == null ? null : source.toString();
		}
	}

	@ReadingConverter
	public enum StringToBigDecimal implements Converter<String, BigDecimal> {
		INSTANCE;

		@Override
		public BigDecimal convert(String source) {
			return source == null ? null : new BigDecimal(source);
		}
	}

	@WritingConverter
	public enum ByteArrayToString implements Converter<byte[], String> {
		INSTANCE;

		@Override
		public String convert(byte[] source) {
			return source == null ? null : Base64Utils.encodeToString(source);
		}
	}

	@ReadingConverter
	public enum StringToByteArray implements Converter<String, byte[]> {
		INSTANCE;

		@Override
		public byte[] convert(String source) {
			return source == null ? null : Base64Utils.decode(source.getBytes(StandardCharsets.UTF_8));
		}
	}

	@WritingConverter
	public enum CharArrayToString implements Converter<char[], String> {
		INSTANCE;

		@Override
		public String convert(char[] source) {
			return source == null ? null : new String(source);
		}
	}

	@ReadingConverter
	public enum StringToCharArray implements Converter<String, char[]> {
		INSTANCE;

		@Override
		public char[] convert(String source) {
			return source == null ? null : source.toCharArray();
		}
	}

	@WritingConverter
	public enum ClassToString implements Converter<Class<?>, String> {
		INSTANCE;

		@Override
		public String convert(Class<?> source) {
			return source == null ? null : source.getClass().getName();
		}
	}

	@ReadingConverter
	public enum StringToClass implements Converter<String, Class<?>> {
		INSTANCE;

		@Override
		public Class<?> convert(String source) {
			try {
				return source == null ? null : Class.forName(source);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Writing converter for Enums. This is registered in
	 * {@link org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration#customConversions( CryptoManager, ObjectMapper)}.
	 * The corresponding reading converters are in {@link IntegerToEnumConverterFactory} and
	 * {@link StringToEnumConverterFactory}
	 */

	@WritingConverter
	public static class EnumToObject implements Converter<Enum<?>, Object> {
		private final ObjectMapper objectMapper;
		private static final JsonFactory factory = new JsonFactory();

		public EnumToObject(ObjectMapper objectMapper) {
			this.objectMapper = objectMapper;
		}

		@Override
		public Object convert(Enum<?> source) {
			try (Writer writer = new StringWriter(); JsonGenerator generator = factory.createGenerator(writer)) {
				objectMapper.writeValue(generator, source);
				String s = writer.toString();
				if (s != null && s.startsWith("\"")) {
					return objectMapper.readValue(s, String.class);
				}
				if ("true".equals(s) || "false".equals(s)) {
					return objectMapper.readValue(s, Boolean.class);
				}
				return objectMapper.readValue(s, Number.class);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

}
