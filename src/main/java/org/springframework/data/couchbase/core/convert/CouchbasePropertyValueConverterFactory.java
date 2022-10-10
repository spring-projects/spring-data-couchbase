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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.mapping.PersistentProperty;

import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.java.encryption.annotation.Encrypted;

/**
 * Accept the Couchbase @Encrypted annotation in addition to @ValueConverter
 *
 * @author Michael Reiche
 */
public class CouchbasePropertyValueConverterFactory implements PropertyValueConverterFactory {

	CryptoManager cryptoManager;
	Map<Class<? extends PropertyValueConverter<?, ?, ?>>, PropertyValueConverter<?, ?, ?>> converterCache = new HashMap<>();

	public CouchbasePropertyValueConverterFactory(CryptoManager cryptoManager) {
		this.cryptoManager = cryptoManager;
	}

	@Override
	public <DV, SV, P extends ValueConversionContext<?>> PropertyValueConverter<DV, SV, P> getConverter(
			PersistentProperty<?> property) {
		PropertyValueConverter<DV, SV, P> valueConverter = PropertyValueConverterFactory.super.getConverter(property);
		if (valueConverter != null) {
			return valueConverter;
		}
		Encrypted encryptedAnn = property.findAnnotation(Encrypted.class);
		if (encryptedAnn != null) {
			Class cryptoConverterClass = CryptoConverter.class;
			return getConverter((Class<PropertyValueConverter<DV, SV, P>>) cryptoConverterClass);
		} else {
			return null;
		}
	}

	@Override
	public <DV, SV, P extends ValueConversionContext<?>> PropertyValueConverter<DV, SV, P> getConverter(
			Class<? extends PropertyValueConverter<DV, SV, P>> converterType) {

		PropertyValueConverter<?, ?, ?> converter = converterCache.get(converterType);
		if (converter != null) {
			return (PropertyValueConverter<DV, SV, P>) converter;
		}

		if (CryptoConverter.class.isAssignableFrom(converterType)) {
			converter = new CryptoConverter(cryptoManager);
		} else {
			try {
				Constructor constructor = converterType.getConstructor();
				converter = (PropertyValueConverter<?, ?, ?>) constructor.newInstance();
			} catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
		converterCache.put((Class<? extends PropertyValueConverter<DV, SV, P>>) converter.getClass(), converter);
		return (PropertyValueConverter<DV, SV, P>) converter;

	}
}
