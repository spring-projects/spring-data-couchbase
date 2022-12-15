/*
 * Copyright 2012-2023 the original author or authors
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

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.BeanUtils;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.mapping.PersistentProperty;

import com.couchbase.client.core.encryption.CryptoManager;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.springframework.util.Assert;

/**
 * Accept the Couchbase @Encrypted and @JsonValue annotations in addition to @ValueConverter annotation.<br>
 * There can only be one propertyValueConverter for a property. Although there maybe be multiple annotations,
 * getConverter(property) only returns one converter (a ChainedPropertyValueConverter might be useful). Note that
 * valueConversions.afterPropertiesSet() (see
 * {@link org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration#customConversions(CryptoManager)}
 * encapsulates this in a CachingPropertyValueConverterFactory which caches by 'property'. Although
 * CachingPropertyValueConverterFactory does have the functionality to cache by a type, it only caches by the type
 * specified on an @ValueConverter annotation.To avoid having identical converter instances for each instance of a class
 * containing an @JsonValue annotation, converterCacheForType is used.
 *
 * @author Michael Reiche
 */
public class CouchbasePropertyValueConverterFactory implements PropertyValueConverterFactory {

	final CryptoManager cryptoManager;
	final Map<Class<? extends Annotation>, Class<?>> annotationToConverterMap;
	static protected final Map<Class<?>, Optional<PropertyValueConverter<?, ?, ?>>> converterCacheForType = new ConcurrentHashMap<>();

	public CouchbasePropertyValueConverterFactory(CryptoManager cryptoManager,
			Map<Class<? extends Annotation>, Class<?>> annotationToConverterMap) {
		this.cryptoManager = cryptoManager;
		this.annotationToConverterMap = annotationToConverterMap;
	}

	/**
	 * @param property must not be {@literal null}.
	 * @return
	 * @param <DV> destination value
	 * @param <SV> source value
	 * @param <P> context
	 */
	@Override
	public <DV, SV, P extends ValueConversionContext<?>> PropertyValueConverter<DV, SV, P> getConverter(
			PersistentProperty<?> property) {

		PropertyValueConverter<DV, SV, P> valueConverter = PropertyValueConverterFactory.super.getConverter(property);
		if (valueConverter != null) {
			return valueConverter;
		}

		// this will return the converter for the first annotation that requires a PropertyValueConverter like @Encrypted
		for (Annotation ann : property.getField().getAnnotations()) {
			Class<?> converterClass = converterFromFieldAnnotation(ann);
			if (converterClass != null) {
				return getConverter((Class<PropertyValueConverter<DV, SV, P>>) converterClass, property);
			}
		}

		if (property.getType().isEnum()) { // Enums have type-based converters for JsonValue/Creator. see OtherConverters.
			return null;
		}

		// Maybe the type of the property has annotations that indicate a converter (like a method with a @JsonValue)
		return (PropertyValueConverter<DV, SV, P>) converterCacheForType
				.computeIfAbsent(property.getType(), p -> Optional.ofNullable((maybeTypePropertyConverter(property))))
				.orElse(null);
	}

	/**
	 * lookup the converter class from the annotation. Analogous to getting the converter class from the value() attribute
	 * of the @ValueProperty annotation
	 * 
	 * @param ann the annotation
	 * @return the class of the converter
	 */
	private Class<?> converterFromFieldAnnotation(Annotation ann) {
		return annotationToConverterMap.get(ann.annotationType());
	}

	<DV, SV, P extends ValueConversionContext<?>> PropertyValueConverter<DV, SV, P> maybeTypePropertyConverter(
			PersistentProperty<?> property) {

		Class<?> type = property.getType();

		// find the annotated method to determine if a converter is required, and cache it.
		Method jsonValueMethod = null;
		for (Method m : type.getDeclaredMethods()) {
			JsonValue jsonValueAnn = m.getAnnotation(JsonValue.class);
			if (jsonValueAnn != null && jsonValueAnn.value()) {
				Class jsonValueConverterClass = converterFromFieldAnnotation(jsonValueAnn);
				if (jsonValueConverterClass != null) {
					jsonValueMethod = m;
					jsonValueMethod.setAccessible(true);
					JsonValueConverter.valueMethodCache.put(type, jsonValueMethod);
					Constructor<?> jsonCreatorMethod = null;
					for (Constructor<?> c : type.getConstructors()) {
						JsonCreator jsonCreatorAnn = c.getAnnotation(JsonCreator.class);
						if (jsonCreatorAnn != null && !jsonCreatorAnn.mode().equals(JsonCreator.Mode.DISABLED)) {
							jsonCreatorMethod = c;
							jsonCreatorMethod.setAccessible(true);
							JsonValueConverter.creatorMethodCache.put(type, jsonCreatorMethod);
							break;
						}
					}
					return getConverter((Class<PropertyValueConverter<DV, SV, P>>) jsonValueConverterClass, property);
				}
			}
		}

		return null; // we didn't find a property value converter to use
	}

	@Override
	public <DV, SV, P extends ValueConversionContext<?>> PropertyValueConverter<DV, SV, P> getConverter(
			Class<? extends PropertyValueConverter<DV, SV, P>> converterType) {
		return getConverter(converterType, null);
	}

	/**
	 * @param converterType
	 * @param property
	 * @return
	 * @param <DV>
	 * @param <SV>
	 * @param <P>
	 */
	public <DV, SV, P extends ValueConversionContext<?>> PropertyValueConverter<DV, SV, P> getConverter(
			Class<? extends PropertyValueConverter<DV, SV, P>> converterType, PersistentProperty<?> property) {

		// CryptoConverter takes a cryptoManager argument
		if (CryptoConverter.class.isAssignableFrom(converterType)) {
			return (PropertyValueConverter<DV, SV, P>) new CryptoConverter(cryptoManager);
		} else if (property != null) { // try constructor that takes PersistentProperty
			try {
				Constructor<?> constructor = converterType.getConstructor(PersistentProperty.class);
				return (PropertyValueConverter<DV, SV, P>) BeanUtils.instantiateClass(constructor, property);
			} catch (NoSuchMethodException e) {}
		}
		// there is no constructor that takes a property, fall-back to no-args constructor
		return BeanUtils.instantiateClass(converterType);
	}
}
