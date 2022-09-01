/*
 * Copyright 2012-2022 the original author or authors
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalGenericConverter;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.data.convert.WritingConverter;

import com.couchbase.client.core.encryption.CryptoManager;

/**
 * Use the cryptomManager to encrypt properties
 *
 * @author Michael Reiche
 */
@WritingConverter
public class EncryptingWritingConverter implements ConditionalGenericConverter {

	CryptoManager cryptoManager;

	public EncryptingWritingConverter(CryptoManager cryptoManager) {
		this.cryptoManager = cryptoManager;
	}

	@Override
	public Set<GenericConverter.ConvertiblePair> getConvertibleTypes() {

		Set<ConvertiblePair> convertiblePairs = new HashSet<>();
		Class<?>[] clazzes = new Class[] { String.class, Integer.class, Long.class, Float.class, Double.class,
				BigInteger.class, BigDecimal.class, Boolean.class, Enum.class };
		for (Class clazz : clazzes) {
			convertiblePairs.add(new ConvertiblePair(clazz, String.class));
		}
		return convertiblePairs;
	}

	@Override
	public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
		if (source == null) {
			return null;
		}
		com.couchbase.client.java.encryption.annotation.Encrypted ann = sourceType
				.getAnnotation(com.couchbase.client.java.encryption.annotation.Encrypted.class);
		Map<Object, Object> result = new HashMap<>();
		result.putAll(cryptoManager.encrypt(source.toString().getBytes(StandardCharsets.UTF_8), ann.encrypter()));
		return new Encrypted(result);
	}

	@Override
	public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
		return sourceType.hasAnnotation(com.couchbase.client.java.encryption.annotation.Encrypted.class);
	}
}
