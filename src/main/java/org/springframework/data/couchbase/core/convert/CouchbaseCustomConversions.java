/*
 * Copyright 2017-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.convert;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.data.convert.PropertyValueConversions;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.convert.PropertyValueConverterRegistrar;
import org.springframework.data.convert.SimplePropertyValueConversions;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.Assert;

import com.couchbase.client.java.encryption.annotation.Encrypted;

/**
 * Value object to capture custom conversion.
 * <p>
 * Types that can be mapped directly onto JSON are considered simple ones, because they neither need deeper inspection
 * nor nested conversion.
 *
 * @author Michael Nitschinger
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Subhashni Balakrishnan
 * @Michael Reiche
 * @see org.springframework.data.convert.CustomConversions
 * @see SimpleTypeHolder
 * @since 2.0
 */
public class CouchbaseCustomConversions extends org.springframework.data.convert.CustomConversions {

	private static final StoreConversions STORE_CONVERSIONS;

	private static final List<Object> STORE_CONVERTERS;

	static {

		List<Object> converters = new ArrayList<>();

		converters.addAll(DateConverters.getConvertersToRegister());
		converters.addAll(CouchbaseJsr310Converters.getConvertersToRegister());
		converters.addAll(OtherConverters.getConvertersToRegister());

		STORE_CONVERTERS = Collections.unmodifiableList(converters);
		STORE_CONVERSIONS = StoreConversions.of(SimpleTypeHolder.DEFAULT, STORE_CONVERTERS);
	}

	/**
	 * Create a new {@link CouchbaseCustomConversions} instance registering the given converters.
	 *
	 * @param converters must not be {@literal null}.
	 */
	public CouchbaseCustomConversions(List<?> converters) {
		this(CouchbaseConverterConfigurationAdapter.from(converters));
	}

	/**
	 * Create a new {@link CouchbaseCustomConversions} given {@link CouchbaseConverterConfigurationAdapter}.
	 *
	 * @param conversionConfiguration must not be {@literal null}.
	 */
	protected CouchbaseCustomConversions(CouchbaseConverterConfigurationAdapter conversionConfiguration) {
		super(conversionConfiguration.createConverterConfiguration());
	}

	/**
	 * Functional style {@link org.springframework.data.convert.CustomConversions} creation giving users a convenient way
	 * of configuring store specific capabilities by providing deferred hooks to what will be configured when creating the
	 * {@link org.springframework.data.convert.CustomConversions#CustomConversions(ConverterConfiguration) instance}.
	 *
	 * @param configurer must not be {@literal null}.
	 */
	public static CouchbaseCustomConversions create(Consumer<CouchbaseConverterConfigurationAdapter> configurer) {
		CouchbaseConverterConfigurationAdapter adapter = new CouchbaseConverterConfigurationAdapter();
		configurer.accept(adapter);
		return new CouchbaseCustomConversions(adapter);
	}

	@Override
	public boolean hasValueConverter(PersistentProperty<?> property) {
		if (property.findAnnotation(Encrypted.class) != null) {
			return true;
		}
		return super.hasValueConverter(property);
	}

	/**
	 * {@link CouchbaseConverterConfigurationAdapter} encapsulates creation of
	 * {@link org.springframework.data.convert.CustomConversions.ConverterConfiguration} with CouchbaseDB specifics.
	 */
	public static class CouchbaseConverterConfigurationAdapter {

		/**
		 * List of {@literal java.time} types having different representation when rendered
		 */
		private static final Set<Class<?>> JAVA_DRIVER_TIME_SIMPLE_TYPES = new HashSet<>(
				Arrays.asList(LocalDate.class, LocalTime.class, LocalDateTime.class));

		private boolean useNativeDriverJavaTimeCodecs = false;
		private final List<Object> customConverters = new ArrayList<>();
		private final PropertyValueConversions internalValueConversion = PropertyValueConversions.simple(it -> {});
		private PropertyValueConversions propertyValueConversions = internalValueConversion;

		/**
		 * Create a {@link CouchbaseConverterConfigurationAdapter} using the provided {@code converters} and our own codecs
		 * for JSR-310 types.
		 *
		 * @param converters must not be {@literal null}.
		 * @return
		 */
		public static CouchbaseConverterConfigurationAdapter from(List<?> converters) {

			Assert.notNull(converters, "Converters must not be null");

			CouchbaseConverterConfigurationAdapter converterConfigurationAdapter = new CouchbaseConverterConfigurationAdapter();
			converterConfigurationAdapter.registerConverters(converters);
			return converterConfigurationAdapter;
		}

		/**
		 * Add a custom {@link Converter} implementation.
		 *
		 * @param converter must not be {@literal null}.
		 * @return this.
		 */
		public CouchbaseConverterConfigurationAdapter registerConverter(Converter<?, ?> converter) {

			Assert.notNull(converter, "Converter must not be null!");
			customConverters.add(converter);
			return this;
		}

		/**
		 * Gateway to register property specific converters.
		 *
		 * @param configurationAdapter must not be {@literal null}.
		 * @return this.
		 */
		public CouchbaseConverterConfigurationAdapter configurePropertyConversions(
				Consumer<PropertyValueConverterRegistrar<CouchbasePersistentProperty>> configurationAdapter) {

			Assert.state(valueConversions() instanceof SimplePropertyValueConversions,
					"Configured PropertyValueConversions does not allow setting custom ConverterRegistry");

			PropertyValueConverterRegistrar propertyValueConverterRegistrar = new PropertyValueConverterRegistrar();
			configurationAdapter.accept(propertyValueConverterRegistrar);

			((SimplePropertyValueConversions) valueConversions())
					.setValueConverterRegistry(propertyValueConverterRegistrar.buildRegistry());
			return this;
		}

		/**
		 * Add a custom {@link ConverterFactory} implementation.
		 *
		 * @param converterFactory must not be {@literal null}.
		 * @return this.
		 */
		public CouchbaseConverterConfigurationAdapter registerConverterFactory(ConverterFactory<?, ?> converterFactory) {

			Assert.notNull(converterFactory, "ConverterFactory must not be null");
			customConverters.add(converterFactory);
			return this;
		}

		/**
		 * Add {@link Converter converters}, {@link ConverterFactory factories},
		 * {@link org.springframework.data.convert.ConverterBuilder.ConverterAware converter-aware objects}, and
		 * {@link GenericConverter generic converters}.
		 *
		 * @param converters must not be {@literal null} nor contain {@literal null} values.
		 * @return this.
		 */
		public CouchbaseConverterConfigurationAdapter registerConverters(Collection<?> converters) {

			Assert.notNull(converters, "Converters must not be null");
			Assert.noNullElements(converters, "Converters must not be null nor contain null values");

			customConverters.addAll(converters);
			return this;
		}

		/**
		 * Add a custom/default {@link PropertyValueConverterFactory} implementation used to serve
		 * {@link PropertyValueConverter}.
		 *
		 * @param converterFactory must not be {@literal null}.
		 * @return this.
		 */
		public CouchbaseConverterConfigurationAdapter registerPropertyValueConverterFactory(
				PropertyValueConverterFactory converterFactory) {

			Assert.state(valueConversions() instanceof SimplePropertyValueConversions,
					"Configured PropertyValueConversions does not allow setting custom ConverterRegistry");

			((SimplePropertyValueConversions) valueConversions()).setConverterFactory(converterFactory);
			return this;
		}

		/**
		 * Optionally set the {@link PropertyValueConversions} to be applied during mapping.
		 * <p>
		 * Use this method if {@link #configurePropertyConversions(Consumer)} and
		 * {@link #registerPropertyValueConverterFactory(PropertyValueConverterFactory)} are not sufficient.
		 *
		 * @param valueConversions must not be {@literal null}.
		 * @return this.
		 */
		public CouchbaseConverterConfigurationAdapter setPropertyValueConversions(
				PropertyValueConversions valueConversions) {

			Assert.notNull(valueConversions, "PropertyValueConversions must not be null");
			this.propertyValueConversions = valueConversions;
			return this;
		}

		PropertyValueConversions valueConversions() {

			if (this.propertyValueConversions == null) {
				this.propertyValueConversions = internalValueConversion;
			}

			return this.propertyValueConversions;
		}

		ConverterConfiguration createConverterConfiguration() {

			if (hasDefaultPropertyValueConversions()
					&& propertyValueConversions instanceof SimplePropertyValueConversions svc) {
				svc.init();
			}

			if (!useNativeDriverJavaTimeCodecs) {
				return new ConverterConfiguration(STORE_CONVERSIONS, this.customConverters, convertiblePair -> true,
						this.propertyValueConversions);
			}

			/*
			 * We need to have those converters using UTC as the default ones would go on with the systemDefault.
			 */
			List<Object> converters = new ArrayList<>(STORE_CONVERTERS.size() + 3);
			converters.add(DateToUtcLocalDateConverter.INSTANCE);
			converters.add(DateToUtcLocalTimeConverter.INSTANCE);
			converters.add(DateToUtcLocalDateTimeConverter.INSTANCE);
			converters.addAll(STORE_CONVERTERS);

			StoreConversions storeConversions = StoreConversions.of(new SimpleTypeHolder(JAVA_DRIVER_TIME_SIMPLE_TYPES,
					SimpleTypeHolder.DEFAULT /* CouchbaseSimpleTypes.HOLDER */), converters);

			return new ConverterConfiguration(storeConversions, this.customConverters, convertiblePair -> {

				// Avoid default registrations

				if (JAVA_DRIVER_TIME_SIMPLE_TYPES.contains(convertiblePair.getSourceType())
						&& Date.class.isAssignableFrom(convertiblePair.getTargetType())) {
					return false;
				}

				return true;
			}, this.propertyValueConversions);
		}

		private enum DateToUtcLocalDateTimeConverter implements Converter<Date, LocalDateTime> {
			INSTANCE;

			@Override
			public LocalDateTime convert(Date source) {
				return LocalDateTime.ofInstant(Instant.ofEpochMilli(source.getTime()), ZoneId.of("UTC"));
			}
		}

		private enum DateToUtcLocalTimeConverter implements Converter<Date, LocalTime> {
			INSTANCE;

			@Override
			public LocalTime convert(Date source) {
				return DateToUtcLocalDateTimeConverter.INSTANCE.convert(source).toLocalTime();
			}
		}

		private enum DateToUtcLocalDateConverter implements Converter<Date, LocalDate> {
			INSTANCE;

			@Override
			public LocalDate convert(Date source) {
				return DateToUtcLocalDateTimeConverter.INSTANCE.convert(source).toLocalDate();
			}
		}

		private boolean hasDefaultPropertyValueConversions() {
			return propertyValueConversions == internalValueConversion;
		}

	}
}
