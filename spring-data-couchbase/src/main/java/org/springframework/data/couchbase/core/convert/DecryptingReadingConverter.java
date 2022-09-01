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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalGenericConverter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;

import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.java.encryption.annotation.Encrypted;

/**
 * Use the cryptoManager to decrypt a field
 * 
 * @author Michael Reiche
 */
@ReadingConverter
public class DecryptingReadingConverter implements ConditionalGenericConverter {

	CryptoManager cryptoManager;
	ConversionService conversionService;

	public DecryptingReadingConverter(CryptoManager cryptoManager) {
		this.cryptoManager = cryptoManager;
	}

	@Override
	public Set<ConvertiblePair> getConvertibleTypes() {
		Set<ConvertiblePair> convertiblePairs = new HashSet<>();
		Class<?>[] clazzes = new Class[] { String.class, Integer.class, Long.class, Float.class, Double.class,
				BigInteger.class, BigDecimal.class, Boolean.class, Enum.class };
		for (Class clazz : clazzes) {
			convertiblePairs.add(new ConvertiblePair(CouchbaseDocument.class, clazz));
		}
		return convertiblePairs;
	}

	@Override
	public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
		return source == null ? null
				: new String(cryptoManager.decrypt(((CouchbaseDocument) source).getContent()), StandardCharsets.UTF_8);
	}

	@Override
	public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
		return targetType.hasAnnotation(Encrypted.class);
	}
}
