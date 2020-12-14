/*
 * Copyright 2012-2020 the original author or authors
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

package org.springframework.data.couchbase.core;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.event.AfterConvertCallback;
import org.springframework.data.couchbase.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.couchbase.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.couchbase.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.couchbase.core.mapping.event.CouchbaseMappingEvent;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.util.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal encode/decode support for CouchbaseTemplate.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jorge Rodriguez Martin
 * @since 3.0
 */
class CouchbaseTemplateSupport implements ApplicationContextAware {

	private static final Logger LOG = LoggerFactory.getLogger(CouchbaseTemplateSupport.class);

	private final CouchbaseConverter converter;
	private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
	private final TranslationService translationService;
	private EntityCallbacks entityCallbacks;
	private ApplicationContext applicationContext;

	public CouchbaseTemplateSupport(final CouchbaseConverter converter, final TranslationService translationService) {
		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
		this.translationService = translationService;
	}

	public CouchbaseDocument encodeEntity(final Object entityToEncode) {
		maybeEmitEvent(new BeforeConvertEvent<>(entityToEncode));
		Object maybeNewEntity = maybeCallBeforeConvert(entityToEncode, "");
		final CouchbaseDocument converted = new CouchbaseDocument();
		converter.write(maybeNewEntity, converted);
		maybeCallAfterConvert(entityToEncode, converted, "");
		maybeEmitEvent(new BeforeSaveEvent<>(entityToEncode, converted));
		return converted;
	}

	public <T> T decodeEntity(String id, String source, long cas, Class<T> entityClass) {
		final CouchbaseDocument converted = new CouchbaseDocument(id);
		converted.setId(id);
		CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(entityClass);
		if (cas != 0 && persistentEntity.getVersionProperty() != null) {
			converted.put(persistentEntity.getVersionProperty().getName(), cas);
		}

		T readEntity = converter.read(entityClass, (CouchbaseDocument) translationService.decode(source, converted));
		final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(readEntity);

		if (persistentEntity.getVersionProperty() != null) {
			accessor.setProperty(persistentEntity.getVersionProperty(), cas);
		}
		return accessor.getBean();
	}

	public Object applyUpdatedCas(final Object entity, final long cas) {
		final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(entity.getClass());
		final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();

		if (versionProperty != null) {
			accessor.setProperty(versionProperty, cas);
			return accessor.getBean();
		}
		return entity;
	}

	public Object applyUpdatedId(final Object entity, Object id) {
		final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(entity.getClass());
		final CouchbasePersistentProperty idProperty = persistentEntity.getIdProperty();

		if (idProperty != null) {
			accessor.setProperty(idProperty, id);
			return accessor.getBean();
		}
		return entity;
	}

	public long getCas(final Object entity) {
		final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(entity.getClass());
		final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();

		long cas = 0;
		if (versionProperty != null) {
			Object casObject = (Number) accessor.getProperty(versionProperty);
			if (casObject instanceof Number) {
				cas = ((Number) casObject).longValue();
			}
		}
		return cas;
	}

	public String getJavaNameForEntity(final Class<?> clazz) {
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(clazz);
		MappingCouchbaseEntityInformation<?, Object> info = new MappingCouchbaseEntityInformation<>(persistentEntity);
		return info.getJavaType().getName();
	}

	private <T> ConvertingPropertyAccessor<T> getPropertyAccessor(final T source) {
		CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(source.getClass());
		PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(source);
		return new ConvertingPropertyAccessor<>(accessor, converter.getConversionService());
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
		if (entityCallbacks == null) {
			setEntityCallbacks(EntityCallbacks.create(applicationContext));
		}
	}

	/**
	 * Set the {@link EntityCallbacks} instance to use when invoking
	 * {@link org.springframework.data.mapping.callback.EntityCallback callbacks} like the {@link BeforeConvertCallback}.
	 * <p/>
	 * Overrides potentially existing {@link EntityCallbacks}.
	 *
	 * @param entityCallbacks must not be {@literal null}.
	 * @throws IllegalArgumentException if the given instance is {@literal null}.
	 * @since 2.2
	 */
	public void setEntityCallbacks(EntityCallbacks entityCallbacks) {
		Assert.notNull(entityCallbacks, "EntityCallbacks must not be null!");
		this.entityCallbacks = entityCallbacks;
	}

	void maybeEmitEvent(CouchbaseMappingEvent<?> event) {
		if (canPublishEvent()) {
			try {
				this.applicationContext.publishEvent(event);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			LOG.info("maybeEmitEvent called, but CouchbaseTemplate not initialized with applicationContext");
		}

	}

	private boolean canPublishEvent() {
		return this.applicationContext != null;
	}

	protected <T> T maybeCallBeforeConvert(T object, String collection) {
		if (entityCallbacks != null) {
			try {
				return entityCallbacks.callback(BeforeConvertCallback.class, object, collection);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			LOG.info("maybeCallBeforeConvert called, but CouchbaseTemplate not initialized with applicationContext");
		}
		return object;
	}

	protected <T> T maybeCallAfterConvert(T object, CouchbaseDocument document, String collection) {
		if (null != entityCallbacks) {
			return entityCallbacks.callback(AfterConvertCallback.class, object, document, collection);
		} else {
			LOG.info("maybeCallAfterConvert called, but CouchbaseTemplate not initialized with applicationContext");
		}
		return object;
	}

}
