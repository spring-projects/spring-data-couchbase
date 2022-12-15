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

import java.util.Collection;
import java.util.Collections;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseList;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;

/**
 * An abstract {@link CouchbaseConverter} that provides the basics for the {@link MappingCouchbaseConverter}.
 *
 * @author Michael Nitschinger
 * @author Mark Paluch
 * @author Michael Reiche
 */
public abstract class AbstractCouchbaseConverter implements CouchbaseConverter, InitializingBean {

	/**
	 * Contains the conversion service.
	 */
	protected final GenericConversionService conversionService;

	/**
	 * Contains the entity instantiators.
	 */
	protected EntityInstantiators instantiators = new EntityInstantiators();

	/**
	 * Holds the custom conversions.
	 */
	protected CustomConversions conversions = new CouchbaseCustomConversions(Collections.emptyList());

	/**
	 * Create a new converter and hand it over the {@link ConversionService}
	 *
	 * @param conversionService the conversion service to use.
	 */
	protected AbstractCouchbaseConverter(final GenericConversionService conversionService) {
		this.conversionService = conversionService;
	}

	/**
	 * Return the conversion service.
	 *
	 * @return the conversion service.
	 */
	@Override
	public ConversionService getConversionService() {
		return conversionService;
	}

	/**
	 * Set the custom conversions. Note that updating conversions requires a subsequent call to register them with the
	 * conversionService: conversions.registerConvertersIn(conversionService)
	 *
	 * @param conversions the conversions.
	 */
	public void setCustomConversions(final CustomConversions conversions) {
		this.conversions = conversions;
	}

	/**
	 * Set the entity instantiators.
	 *
	 * @param instantiators the instantiators.
	 */
	public void setInstantiators(final EntityInstantiators instantiators) {
		this.instantiators = instantiators;
	}

	/**
	 * Do nothing after the properties set on the bean.
	 */
	@Override
	public void afterPropertiesSet() {
		conversions.registerConvertersIn(conversionService);
	}

	/**
	 * This convertForWriteIfNeeded takes a property and accessor so that the annotations can be accessed (ie. @Encrypted)
	 *
	 * @param prop the property to be converted to the class that would actually be stored.
	 * @param accessor the property accessor
	 * @return
	 */
	@Override
	public Object convertForWriteIfNeeded(CouchbasePersistentProperty prop, ConvertingPropertyAccessor<Object> accessor,
			boolean processValueConverter) {
		Object value = accessor.getProperty(prop, prop.getType());
		if (value == null) {
			return null;
		}
		if (processValueConverter && conversions.hasValueConverter(prop)) {
			return conversions.getPropertyValueConversions().getValueConverter(prop).write(value,
					new CouchbaseConversionContext(prop, (MappingCouchbaseConverter) this, accessor));
		}
		Class<?> targetClass = this.conversions.getCustomWriteTarget(value.getClass()).orElse(null);

		boolean canConvert = targetClass == null ? false
				: this.conversionService.canConvert(new TypeDescriptor(prop.getField()), TypeDescriptor.valueOf(targetClass));
		if (canConvert) {
			return this.conversionService.convert(value, new TypeDescriptor(prop.getField()),
					TypeDescriptor.valueOf(targetClass));
		}

		Object result = this.conversions.getCustomWriteTarget(prop.getType()) //
				.map(it -> this.conversionService.convert(value, new TypeDescriptor(prop.getField()),
						TypeDescriptor.valueOf(it))) //
				.orElse(value);
		// superseded by Enum converters
		// .orElseGet(() -> Enum.class.isAssignableFrom(value.getClass()) ? ((Enum<?>) value).name() : value);

		return result;

	}

	/**
	 * This convertForWriteIfNeed takes only the value to convert. It cannot access the annotations of the Field being
	 * converted.
	 *
	 * @param inValue the value to be converted to the class that would actually be stored.
	 * @return
	 */
	@Override
	public Object convertForWriteIfNeeded(Object inValue) {
		if (inValue == null) {
			return null;
		}

		Object value = this.conversions.getCustomWriteTarget(inValue.getClass()) //
				.map(it -> (Object) this.conversionService.convert(inValue, it)) //
				.orElse(inValue);

		Class<?> elementType = value.getClass();

		if (elementType == null || conversions.isSimpleType(elementType)) {
			// superseded by EnumCvtrs value = Enum.class.isAssignableFrom(value.getClass()) ? ((Enum<?>) value).name() : value;
		} else if (value instanceof Collection || elementType.isArray()) {
			TypeInformation<?> type = ClassTypeInformation.from(value.getClass());
			value = ((MappingCouchbaseConverter) this).writeCollectionInternal(MappingCouchbaseConverter.asCollection(value),
					new CouchbaseList(conversions.getSimpleTypeHolder()), type, null, null);
		} else {
			CouchbaseDocument embeddedDoc = new CouchbaseDocument();
			TypeInformation<?> type = ClassTypeInformation.from(value.getClass());
			((MappingCouchbaseConverter) this).writeInternalRoot(value, embeddedDoc, type, false, null);
			value = embeddedDoc;
		}
		return value;
	}

	@Override
	public Class<?> getWriteClassFor(Class<?> clazz) {
		return this.conversions.getCustomWriteTarget(clazz).orElse(clazz);
	}

	@Override
	public CustomConversions getConversions() {
		return conversions;
	}
}
