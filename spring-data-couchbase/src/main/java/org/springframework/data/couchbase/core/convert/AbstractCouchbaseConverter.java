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

import java.util.Collections;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.model.EntityInstantiators;

/**
 * An abstract {@link CouchbaseConverter} that provides the basics for the {@link MappingCouchbaseConverter}.
 *
 * @author Michael Nitschinger
 * @author Mark Paluch
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

	@Override
	public Object convertForWriteIfNeeded(Object value) {
		if (value == null) {
			return null;
		}

		return this.conversions.getCustomWriteTarget(value.getClass()) //
				.map(it -> (Object) this.conversionService.convert(value, it)) //
				.orElseGet(() -> Enum.class.isAssignableFrom(value.getClass()) ? ((Enum<?>) value).name() : value);

	}

	/* TODO needed later
	@Override
	public Object convertToCouchbaseType(Object value,  TypeInformation<?> typeInformation) {
		if (value == null) {
			return null;
		}
	
		return this.conversions.getCustomWriteTarget(value.getClass()) //
				.map(it -> (Object) this.conversionService.convert(value, it)) //
				.orElseGet(() -> Enum.class.isAssignableFrom(value.getClass()) ? ((Enum<?>) value).name() : value);
	
	}
	
	@Override
	public Object convertToCouchbaseType(String source) {
		return source;
	}
	*/

	@Override
	public Class<?> getWriteClassFor(Class<?> clazz) {
		return this.conversions.getCustomWriteTarget(clazz).orElse(clazz);
	}
}
