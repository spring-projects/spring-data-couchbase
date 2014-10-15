/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.convert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.GenericTypeResolver;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.JodaTimeConverters;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Value object to capture custom conversion.
 *
 * <p>Types that can be mapped directly onto JSON are considered simple ones, because they neither need deeper
 * inspection nor nested conversion.</p>
 *
 * @author Michael Nitschinger
 * @author Oliver Gierke
 */
public class CustomConversions {

  private static final Logger LOG = LoggerFactory.getLogger(CustomConversions.class);
  private static final String READ_CONVERTER_NOT_SIMPLE = "Registering converter from %s to %s as reading converter although it doesn't convert from a Couchbase supported type! You might wanna check you annotation setup at the converter implementation.";
  private static final String WRITE_CONVERTER_NOT_SIMPLE = "Registering converter from %s to %s as writing converter although it doesn't convert to a Couchbase supported type! You might wanna check you annotation setup at the converter implementation.";

  /**
   * Contains the simple type holder.
   */
  private final SimpleTypeHolder simpleTypeHolder;

  private final List<Object> converters;

  private final Set<GenericConverter.ConvertiblePair> readingPairs;
  private final Set<GenericConverter.ConvertiblePair> writingPairs;
  private final Set<Class<?>> customSimpleTypes;
  private final ConcurrentMap<GenericConverter.ConvertiblePair, CacheValue> customReadTargetTypes;

  /**
   * Create a new instance with no converters.
   */
  CustomConversions() {
    this(new ArrayList<Object>());
  }

  /**
   * Create a new instance with a given list of conversers.
   * @param converters the list of custom converters.
   */
  public CustomConversions(final List<?> converters) {
    Assert.notNull(converters);

    readingPairs = new LinkedHashSet<GenericConverter.ConvertiblePair>();
    writingPairs = new LinkedHashSet<GenericConverter.ConvertiblePair>();
    customSimpleTypes = new HashSet<Class<?>>();
    customReadTargetTypes = new ConcurrentHashMap<GenericConverter.ConvertiblePair, CacheValue>();

    this.converters = new ArrayList<Object>();
    this.converters.addAll(converters);
    this.converters.addAll(DateConverters.getConvertersToRegister());

    for (Object converter : this.converters) {
      registerConversion(converter);
    }

    simpleTypeHolder = new SimpleTypeHolder(customSimpleTypes, true);
  }

  /**
   * Check that the given type is of "simple type".
   *
   * @param type the type to check.
   * @return if its simple type or not.
   */
  public boolean isSimpleType(final Class<?> type) {
    return simpleTypeHolder.isSimpleType(type);
  }

  /**
   * Returns the simple type holder.
   *
   * @return the simple type holder.
   */
  public SimpleTypeHolder getSimpleTypeHolder() {
    return simpleTypeHolder;
  }

  /**
   * Populates the given {@link GenericConversionService} with the convertes registered.
   *
   * @param conversionService the service to register.
   */
  public void registerConvertersIn(final GenericConversionService conversionService) {
    for (Object converter : converters) {
      boolean added = false;

      if (converter instanceof Converter) {
        conversionService.addConverter((Converter<?, ?>) converter);
        added = true;
      }

      if (converter instanceof ConverterFactory) {
        conversionService.addConverterFactory((ConverterFactory<?, ?>) converter);
        added = true;
      }

      if (converter instanceof GenericConverter) {
        conversionService.addConverter((GenericConverter) converter);
        added = true;
      }

      if (!added) {
        throw new IllegalArgumentException("Given set contains element that is neither Converter nor ConverterFactory!");
      }
    }
  }

  /**
   * Registers a conversion for the given converter. Inspects either generics or the convertible pairs returned
   * by a {@link GenericConverter}.
   *
   * @param converter the converter to register.
   */
  private void registerConversion(final Object converter) {
    Class<?> type = converter.getClass();
    boolean isWriting = type.isAnnotationPresent(WritingConverter.class);
    boolean isReading = type.isAnnotationPresent(ReadingConverter.class);

    if (converter instanceof GenericConverter) {
      GenericConverter genericConverter = (GenericConverter) converter;
      for (GenericConverter.ConvertiblePair pair : genericConverter.getConvertibleTypes()) {
        register(new ConverterRegistration(pair, isReading, isWriting));
      }
    } else if (converter instanceof Converter) {
      Class<?>[] arguments = GenericTypeResolver.resolveTypeArguments(converter.getClass(), Converter.class);
      register(new ConverterRegistration(arguments[0], arguments[1], isReading, isWriting));
    } else {
      throw new IllegalArgumentException("Unsupported Converter type!");
    }
  }

  /**
   * Registers the given {@link ConverterRegistration} as reading or writing pair depending on the type sides being basic
   * Couchbase types.
   *
   * @param registration the registration.
   */
  private void register(final ConverterRegistration registration) {
    GenericConverter.ConvertiblePair pair = registration.getConvertiblePair();

    if (registration.isReading()) {
      readingPairs.add(pair);
      if (LOG.isWarnEnabled() && !registration.isSimpleSourceType()) {
        LOG.warn(String.format(READ_CONVERTER_NOT_SIMPLE, pair.getSourceType(), pair.getTargetType()));
      }
    }

    if (registration.isWriting()) {
      writingPairs.add(pair);
      customSimpleTypes.add(pair.getSourceType());
      if (LOG.isWarnEnabled() && !registration.isSimpleTargetType()) {
        LOG.warn(String.format(WRITE_CONVERTER_NOT_SIMPLE, pair.getSourceType(), pair.getTargetType()));
      }
    }
  }

  /**
   * Returns the target type to convert to in case we have a custom conversion registered to convert the given source
   * type into a Couchbase native one.
   *
   * @param sourceType must not be {@literal null}
   * @return
   */
  public Class<?> getCustomWriteTarget(Class<?> sourceType) {
    return getCustomWriteTarget(sourceType, null);
  }

  /**
   * Returns the target type we can write an object of the given source type to. The returned type might be a subclass
   * oth the given expected type though. If {@code expectedTargetType} is {@literal null} we will simply return the
   * first target type matching or {@literal null} if no conversion can be found.
   *
   * @param sourceType must not be {@literal null}
   * @param requestedTargetType
   * @return
   */
  public Class<?> getCustomWriteTarget(Class<?> sourceType, Class<?> requestedTargetType) {
    Assert.notNull(sourceType);
    return getCustomTarget(sourceType, requestedTargetType, writingPairs);
  }

  /**
   * Returns whether we have a custom conversion registered to write into a Couchbase native type. The returned type might
   * be a subclass of the given expected type though.
   *
   * @param sourceType must not be {@literal null}
   * @return
   */
  public boolean hasCustomWriteTarget(Class<?> sourceType) {
    Assert.notNull(sourceType);
    return hasCustomWriteTarget(sourceType, null);
  }

  /**
   * Returns whether we have a custom conversion registered to write an object of the given source type into an object
   * of the given Couchbase native target type.
   *
   * @param sourceType must not be {@literal null}.
   * @param requestedTargetType
   * @return
   */
  public boolean hasCustomWriteTarget(Class<?> sourceType, Class<?> requestedTargetType) {
    Assert.notNull(sourceType);
    return getCustomWriteTarget(sourceType, requestedTargetType) != null;
  }

  /**
   * Returns whether we have a custom conversion registered to read the given source into the given target type.
   *
   * @param sourceType must not be {@literal null}
   * @param requestedTargetType must not be {@literal null}
   * @return
   */
  public boolean hasCustomReadTarget(Class<?> sourceType, Class<?> requestedTargetType) {
    Assert.notNull(sourceType);
    Assert.notNull(requestedTargetType);
    return getCustomReadTarget(sourceType, requestedTargetType) != null;
  }

  /**
   * Returns the actual target type for the given {@code sourceType} and {@code requestedTargetType}. Note that the
   * returned {@link Class} could be an assignable type to the given {@code requestedTargetType}.
   *
   * @param sourceType must not be {@literal null}.
   * @param requestedTargetType can be {@literal null}.
   * @return
   */
  private Class<?> getCustomReadTarget(Class<?> sourceType, Class<?> requestedTargetType) {
    Assert.notNull(sourceType);
    if (requestedTargetType == null) {
      return null;
    }

    GenericConverter.ConvertiblePair lookupKey = new GenericConverter.ConvertiblePair(sourceType, requestedTargetType);
    CacheValue readTargetTypeValue = customReadTargetTypes.get(lookupKey);

    if (readTargetTypeValue != null) {
      return readTargetTypeValue.getType();
    }

    readTargetTypeValue = CacheValue.of(getCustomTarget(sourceType, requestedTargetType, readingPairs));
    CacheValue cacheValue = customReadTargetTypes.putIfAbsent(lookupKey, readTargetTypeValue);

    return cacheValue != null ? cacheValue.getType() : readTargetTypeValue.getType();
  }

  /**
   * Inspects the given {@link org.springframework.core.convert.converter.GenericConverter.ConvertiblePair} for ones
   * that have a source compatible type as source. Additionally checks assignability of the target type if one is
   * given.
   *
   * @param sourceType must not be {@literal null}.
   * @param requestedTargetType can be {@literal null}.
   * @param pairs must not be {@literal null}.
   * @return
   */
  private static Class<?> getCustomTarget(Class<?> sourceType, Class<?> requestedTargetType,
    Iterable<GenericConverter.ConvertiblePair> pairs) {
    Assert.notNull(sourceType);
    Assert.notNull(pairs);

    for (GenericConverter.ConvertiblePair typePair : pairs) {
      if (typePair.getSourceType().isAssignableFrom(sourceType)) {
        Class<?> targetType = typePair.getTargetType();
        if (requestedTargetType == null || targetType.isAssignableFrom(requestedTargetType)) {
          return targetType;
        }
      }
    }

    return null;
  }

  /**
   * Wrapper to safely store {@literal null} values in the type cache.
   *
   * @author Patryk Wasik
   * @author Oliver Gierke
   * @author Thomas Darimont
   */
  private static class CacheValue {

    private static final CacheValue ABSENT = new CacheValue(null);

    private final Class<?> type;

    public CacheValue(Class<?> type) {
      this.type = type;
    }

    public Class<?> getType() {
      return type;
    }

    static CacheValue of(Class<?> type) {
      return type == null ? ABSENT : new CacheValue(type);
    }
  }
}
