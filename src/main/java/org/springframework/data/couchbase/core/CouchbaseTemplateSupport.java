/*
/*
 * Copyright 2012-2021 the original author or authors
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

import java.lang.reflect.InaccessibleObjectException;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.join.N1qlJoinResolver;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.event.AfterConvertCallback;
import org.springframework.data.couchbase.core.mapping.event.AfterSaveEvent;
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
import org.springframework.util.ClassUtils;

/**
 * Internal encode/decode support for CouchbaseTemplate.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jorge Rodriguez Martin
 * @author Carlos Espinaco
 * @since 3.0
 */
class CouchbaseTemplateSupport implements ApplicationContextAware, TemplateSupport {

	private static final Logger LOG = LoggerFactory.getLogger(CouchbaseTemplateSupport.class);

	private final CouchbaseTemplate template;
	private final CouchbaseConverter converter;
	private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
	private final TranslationService translationService;
	private EntityCallbacks entityCallbacks;
	private ApplicationContext applicationContext;

	public CouchbaseTemplateSupport(final CouchbaseTemplate template, final CouchbaseConverter converter,
			final TranslationService translationService) {
		this.template = template;
		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
		this.translationService = translationService;
	}

	@Override
	public CouchbaseDocument encodeEntity(final Object entityToEncode) {
		maybeEmitEvent(new BeforeConvertEvent<>(entityToEncode));
		Object maybeNewEntity = maybeCallBeforeConvert(entityToEncode, "");
		final CouchbaseDocument converted = new CouchbaseDocument();
		converter.write(maybeNewEntity, converted);
		maybeCallAfterConvert(entityToEncode, converted, "");
		maybeEmitEvent(new BeforeSaveEvent<>(entityToEncode, converted));
		return converted;
	}

	@Override
	public <T> T decodeEntity(String id, String source, long cas, Class<T> entityClass) {
		final CouchbaseDocument converted = new CouchbaseDocument(id);
		converted.setId(id);
		CouchbasePersistentEntity persistentEntity = couldBePersistentEntity(entityClass);

		if (persistentEntity == null) { // method could return a Long, Boolean, String etc.
			// QueryExecutionConverters.unwrapWrapperTypes will recursively unwrap until there is nothing left
			// to unwrap. This results in List<String[]> being unwrapped past String[] to String, so this may also be a
			// Collection (or Array) of entityClass. We have no way of knowing - so just assume it is what we are told.
			// if this is a Collection or array, only the first element will be returned.
			Set<Map.Entry<String, Object>> set = ((CouchbaseDocument) translationService.decode(source, converted))
					.getContent().entrySet();
			return (T) set.iterator().next().getValue();
		}

		if (cas != 0 && persistentEntity.getVersionProperty() != null) {
			converted.put(persistentEntity.getVersionProperty().getName(), cas);
		}

		T readEntity = converter.read(entityClass, (CouchbaseDocument) translationService.decode(source, converted));
		final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(readEntity);

		if (persistentEntity.getVersionProperty() != null) {
			accessor.setProperty(persistentEntity.getVersionProperty(), cas);
		}
		N1qlJoinResolver.handleProperties(persistentEntity, accessor, template.reactive(), id);
		return accessor.getBean();
	}
	CouchbasePersistentEntity couldBePersistentEntity(Class<?> entityClass) {
		if (ClassUtils.isPrimitiveOrWrapper(entityClass) || entityClass == String.class) {
			return null;
		}
		try {
			return mappingContext.getPersistentEntity(entityClass);
		} catch (InaccessibleObjectException t) {

		}
		return null;
	}

	@Override
	public Object applyUpdatedCas(final Object entity, CouchbaseDocument converted, final long cas) {
		Object returnValue;
		final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(entity.getClass());
		final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();

		if (versionProperty != null) {
			accessor.setProperty(versionProperty, cas);
			returnValue = accessor.getBean();
		} else {
			returnValue = entity;
		}
		maybeEmitEvent(new AfterSaveEvent(returnValue, converted));

		return returnValue;
	}

	@Override
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

	@Override
	public long getCas(final Object entity) {
		final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(entity.getClass());
		final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();

		long cas = 0;
		if (versionProperty != null) {
			Object casObject = accessor.getProperty(versionProperty);
			if (casObject instanceof Number) {
				cas = ((Number) casObject).longValue();
			}
		}
		return cas;
	}

	@Override
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

	public void maybeEmitEvent(CouchbaseMappingEvent<?> event) {
		if (canPublishEvent()) {
			try {
				this.applicationContext.publishEvent(event);
			} catch (Exception e) {
				LOG.warn("{} thrown during {}", e, event);
				throw e;
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
			return entityCallbacks.callback(BeforeConvertCallback.class, object, collection);
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
