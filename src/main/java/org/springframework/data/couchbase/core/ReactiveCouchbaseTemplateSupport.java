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

import reactor.core.publisher.Mono;

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
import org.springframework.data.couchbase.core.mapping.event.AfterSaveEvent;
import org.springframework.data.couchbase.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.couchbase.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.couchbase.core.mapping.event.CouchbaseMappingEvent;
import org.springframework.data.couchbase.core.mapping.event.ReactiveAfterConvertCallback;
import org.springframework.data.couchbase.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Internal encode/decode support for {@link ReactiveCouchbaseTemplate}.
 *
 * @author Carlos Espinaco
 * @since 4.2
 */
class ReactiveCouchbaseTemplateSupport implements ApplicationContextAware, ReactiveTemplateSupport {

	private static final Logger LOG = LoggerFactory.getLogger(ReactiveCouchbaseTemplateSupport.class);

	private final ReactiveCouchbaseTemplate template;
	private final CouchbaseConverter converter;
	private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
	private final TranslationService translationService;
	private ReactiveEntityCallbacks reactiveEntityCallbacks;
	private ApplicationContext applicationContext;

	public ReactiveCouchbaseTemplateSupport(final ReactiveCouchbaseTemplate template, final CouchbaseConverter converter,
			final TranslationService translationService) {
		this.template = template;
		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
		this.translationService = translationService;
	}

	@Override
	public Mono<CouchbaseDocument> encodeEntity(final Object entityToEncode) {
		return Mono.just(entityToEncode).doOnNext(entity -> maybeEmitEvent(new BeforeConvertEvent<>(entity)))
				.flatMap(entity -> maybeCallBeforeConvert(entity, "")).map(maybeNewEntity -> {
					final CouchbaseDocument converted = new CouchbaseDocument();
					converter.write(maybeNewEntity, converted);
					return converted;
				}).flatMap(converted -> maybeCallAfterConvert(entityToEncode, converted, "").thenReturn(converted))
				.doOnNext(converted -> maybeEmitEvent(new BeforeSaveEvent<>(entityToEncode, converted)));
	}

	@Override
	public <T> Mono<T> decodeEntity(String id, String source, long cas, Class<T> entityClass) {
		return Mono.fromSupplier(() -> {
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
			N1qlJoinResolver.handleProperties(persistentEntity, accessor, template, id);
			return accessor.getBean();
		});
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
	public Mono<Object> applyUpdatedCas(final Object entity, CouchbaseDocument converted, final long cas) {
		return Mono.fromSupplier(() -> {
			Object returnValue;
			final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);
			final CouchbasePersistentEntity<?> persistentEntity = mappingContext
					.getRequiredPersistentEntity(entity.getClass());
			final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();

			if (versionProperty != null) {
				accessor.setProperty(versionProperty, cas);
				returnValue = accessor.getBean();
			} else {
				returnValue = entity;
			}
			maybeEmitEvent(new AfterSaveEvent(returnValue, converted));
			return returnValue;
		});
	}

	@Override
	public Mono<Object> applyUpdatedId(final Object entity, Object id) {
		return Mono.fromSupplier(() -> {
			final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);
			final CouchbasePersistentEntity<?> persistentEntity = mappingContext
					.getRequiredPersistentEntity(entity.getClass());
			final CouchbasePersistentProperty idProperty = persistentEntity.getIdProperty();

			if (idProperty != null) {
				accessor.setProperty(idProperty, id);
				return accessor.getBean();
			}
			return entity;
		});
	}

	@Override
	public Long getCas(final Object entity) {
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
		if (reactiveEntityCallbacks == null) {
			setReactiveEntityCallbacks(ReactiveEntityCallbacks.create(applicationContext));
		}
	}

	/**
	 * Set the {@link ReactiveEntityCallbacks} instance to use when invoking
	 * {@link org.springframework.data.mapping.callback.ReactiveEntityCallbacks callbacks} like the
	 * {@link ReactiveBeforeConvertCallback}.
	 * <p/>
	 * Overrides potentially existing {@link EntityCallbacks}.
	 *
	 * @param reactiveEntityCallbacks must not be {@literal null}.
	 * @throws IllegalArgumentException if the given instance is {@literal null}.
	 */
	public void setReactiveEntityCallbacks(ReactiveEntityCallbacks reactiveEntityCallbacks) {
		Assert.notNull(reactiveEntityCallbacks, "EntityCallbacks must not be null!");
		this.reactiveEntityCallbacks = reactiveEntityCallbacks;
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
			LOG.info("maybeEmitEvent called, but ReactiveCouchbaseTemplate not initialized with applicationContext");
		}

	}

	private boolean canPublishEvent() {
		return this.applicationContext != null;
	}

	protected <T> Mono<T> maybeCallBeforeConvert(T object, String collection) {
		if (reactiveEntityCallbacks != null) {
			return reactiveEntityCallbacks.callback(ReactiveBeforeConvertCallback.class, object, collection);
		} else {
			LOG.info("maybeCallBeforeConvert called, but ReactiveCouchbaseTemplate not initialized with applicationContext");
		}
		return Mono.just(object);
	}

	protected <T> Mono<T> maybeCallAfterConvert(T object, CouchbaseDocument document, String collection) {
		if (null != reactiveEntityCallbacks) {
			return reactiveEntityCallbacks.callback(ReactiveAfterConvertCallback.class, object, document, collection);
		} else {
			LOG.info("maybeCallAfterConvert called, but ReactiveCouchbaseTemplate not initialized with applicationContext");
		}
		return Mono.just(object);
	}

}
