/*
 * Copyright 2021 the original author or authors
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.join.N1qlJoinResolver;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.event.AfterSaveEvent;
import org.springframework.data.couchbase.core.mapping.event.CouchbaseMappingEvent;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import org.springframework.data.couchbase.transaction.CouchbaseResourceHolder;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;

import org.springframework.util.ClassUtils;

import java.util.Map;
import java.util.Set;

public abstract class AbstractTemplateSupport {

	final ReactiveCouchbaseTemplate template;
	final CouchbaseConverter converter;
	final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
	final TranslationService translationService;
	ApplicationContext applicationContext;
	static final Logger LOG = LoggerFactory.getLogger(AbstractTemplateSupport.class);

	public AbstractTemplateSupport(ReactiveCouchbaseTemplate template, CouchbaseConverter converter, TranslationService translationService) {
		this.template = template;
		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
		this.translationService = translationService;
	}

	abstract ReactiveCouchbaseTemplate getReactiveTemplate();

	public <T> T decodeEntityBase(String id, String source, long cas, Class<T> entityClass, String scope, String collection,
								  TransactionResultHolder txResultHolder, CouchbaseResourceHolder holder) {
		final CouchbaseDocument converted = new CouchbaseDocument(id);
		converted.setId(id);

		// this is the entity class defined for the repository. It may not be the class of the document that was read
		// we will reset it after reading the document
		//
		// This will fail for the case where:
		// 1) The version is defined in the concrete class, but not in the abstract class; and
		// 2) The constructor takes a "long version" argument resulting in an exception would be thrown if version in
		// the source is null.
		// We could expose from the MappingCouchbaseConverter determining the persistent entity from the source,
		// but that is a lot of work to do every time just for this very rare and avoidable case.
		// TypeInformation<? extends R> typeToUse = typeMapper.readType(source, type);

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

		// if possible, set the version property in the source so that if the constructor has a long version argument,
		// it will have a value an not fail (as null is not a valid argument for a long argument). This possible failure
		// can be avoid by defining the argument as Long instead of long.
		// persistentEntity is still the (possibly abstract) class specified in the repository definition
		// it's possible that the abstract class does not have a version property, and this won't be able to set the version
		if (cas != 0 && persistentEntity.getVersionProperty() != null) {
			converted.put(persistentEntity.getVersionProperty().getName(), cas);
		}

		// if the constructor has an argument that is long version, then construction will fail if the 'version'
		// is not available as 'null' is not a legal value for a long. Changing the arg to "Long version" would solve this.
		// (Version doesn't come from 'source', it comes from the cas argument to decodeEntity)
		T readEntity = converter.read(entityClass, (CouchbaseDocument) translationService.decode(source, converted));
		final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(readEntity);

		persistentEntity = couldBePersistentEntity(readEntity.getClass());

		if (cas != 0 && persistentEntity.getVersionProperty() != null) {
			accessor.setProperty(persistentEntity.getVersionProperty(), cas);
		}
		N1qlJoinResolver.handleProperties(persistentEntity, accessor, getReactiveTemplate(), id, scope, collection);

		if(holder != null){
			holder.transactionResultHolder(txResultHolder, (T)accessor.getBean());
		}

		return accessor.getBean();
	}

	CouchbasePersistentEntity couldBePersistentEntity(Class<?> entityClass) {
		if (ClassUtils.isPrimitiveOrWrapper(entityClass) || entityClass == String.class) {
			return null;
		}
		return mappingContext.getPersistentEntity(entityClass);
	}



	public <T> T applyResultBase(T entity, CouchbaseDocument converted, Object id, long cas,
								 TransactionResultHolder txResultHolder, CouchbaseResourceHolder holder) {
		ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);

		final CouchbasePersistentEntity<?> persistentEntity = converter.getMappingContext()
				.getRequiredPersistentEntity(entity.getClass());

		final CouchbasePersistentProperty idProperty = persistentEntity.getIdProperty();
		if (idProperty != null) {
			accessor.setProperty(idProperty, id);
		}

		final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();
		if (versionProperty != null) {
			accessor.setProperty(versionProperty, cas);
		}

		if(holder != null){
			holder.transactionResultHolder(txResultHolder, (T)accessor.getBean());
		}
		maybeEmitEvent(new AfterSaveEvent(accessor.getBean(), converted));
		return (T) accessor.getBean();

	}

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

	public Object getId(final Object entity) {
		final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(entity.getClass());
		final CouchbasePersistentProperty idProperty = persistentEntity.getIdProperty();
		Object id = null;
		if (idProperty != null) {
			id = accessor.getProperty(idProperty);
		}
		return id;
	}

	public String getJavaNameForEntity(final Class<?> clazz) {
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(clazz);
		MappingCouchbaseEntityInformation<?, Object> info = new MappingCouchbaseEntityInformation<>(persistentEntity);
		return info.getJavaType().getName();
	}

	<T> ConvertingPropertyAccessor<T> getPropertyAccessor(final T source) {
		CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(source.getClass());
		PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(source);
		return new ConvertingPropertyAccessor<>(accessor, converter.getConversionService());
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

	public TranslationService getTranslationService(){
		return translationService;
	}
}
