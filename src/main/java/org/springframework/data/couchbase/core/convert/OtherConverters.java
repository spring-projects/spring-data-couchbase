/*
 * Copyright 2021 the original author or authors
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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

/**
 * Out of the box conversions for java dates and calendars.
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
}
