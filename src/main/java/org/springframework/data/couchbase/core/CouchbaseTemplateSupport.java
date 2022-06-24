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
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.event.AfterConvertCallback;
import org.springframework.data.couchbase.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.couchbase.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.couchbase.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.couchbase.core.mapping.event.CouchbaseMappingEvent;
import org.springframework.data.couchbase.core.support.TemplateUtils;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.CouchbaseException;

/**
 * Internal encode/decode support for CouchbaseTemplate.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jorge Rodriguez Martin
 * @author Carlos Espinaco
 * @since 3.0
 */
class CouchbaseTemplateSupport extends AbstractTemplateSupport implements ApplicationContextAware, TemplateSupport {

	private final CouchbaseTemplate template;
	private EntityCallbacks entityCallbacks;

	public CouchbaseTemplateSupport(final CouchbaseTemplate template, final CouchbaseConverter converter,
									final TranslationService translationService) {
		super(template.reactive(), converter, translationService);
		this.template = template;
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
	public <T> T decodeEntity(String id, String source, Long cas, Class<T> entityClass, String scope, String collection) {

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
			final CouchbaseDocument converted = new CouchbaseDocument(id);
			Set<Map.Entry<String, Object>> set = ((CouchbaseDocument) translationService.decode(source, converted))
					.getContent().entrySet();
			return (T) set.iterator().next().getValue();
		}

		if (id == null) {
			throw new CouchbaseException(TemplateUtils.SELECT_ID + " was null. Either use #{#n1ql.selectEntity} or project "
					+ TemplateUtils.SELECT_ID);
		}

		final CouchbaseDocument converted = new CouchbaseDocument(id);

		// if possible, set the version property in the source so that if the constructor has a long version argument,
		// it will have a value and not fail (as null is not a valid argument for a long argument). This possible failure
		// can be avoid by defining the argument as Long instead of long.
		// persistentEntity is still the (possibly abstract) class specified in the repository definition
		// it's possible that the abstract class does not have a version property, and this won't be able to set the version
		if (persistentEntity.getVersionProperty() != null) {
			if (cas == null) {
				throw new CouchbaseException("version/cas in the entity but " + TemplateUtils.SELECT_CAS
						+ " was not in result. Either use #{#n1ql.selectEntity} or project " + TemplateUtils.SELECT_CAS);
			}
			if (cas != 0) {
				converted.put(persistentEntity.getVersionProperty().getName(), cas);
			}
		}

		// if the constructor has an argument that is long version, then construction will fail if the 'version'
		// is not available as 'null' is not a legal value for a long. Changing the arg to "Long version" would solve this.
		// (Version doesn't come from 'source', it comes from the cas argument to decodeEntity)
		T readEntity = converter.read(entityClass, (CouchbaseDocument) translationService.decode(source, converted));
		final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(readEntity);

		persistentEntity = couldBePersistentEntity(readEntity.getClass());

		if (cas != null && cas != 0 && persistentEntity.getVersionProperty() != null) {
			accessor.setProperty(persistentEntity.getVersionProperty(), cas);
		}
		N1qlJoinResolver.handleProperties(persistentEntity, accessor, template.reactive(), id, scope, collection);
		return accessor.getBean();
	}
	CouchbasePersistentEntity couldBePersistentEntity(Class<?> entityClass) {
		if (ClassUtils.isPrimitiveOrWrapper(entityClass) || entityClass == String.class) {
			return null;
		}
		return mappingContext.getPersistentEntity(entityClass);
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
	public <T> T decodeEntity(String id, String source, long cas, Class<T> entityClass, String scope, String collection,
							  TransactionResultHolder txHolder) {
		return decodeEntity(id, source, cas, entityClass, scope, collection, txHolder);
	}

	@Override
	public <T> T decodeEntity(String id, String source, long cas, Class<T> entityClass, String scope, String collection,
							  TransactionResultHolder txHolder, CouchbaseResourceHolder holder) {
		return decodeEntityBase(id, source, cas, entityClass, scope, collection, txHolder, holder);
	}

	@Override
	public <T> T applyResult(T entity, CouchbaseDocument converted, Object id, long cas,
							 TransactionResultHolder txResultHolder) {
		return applyResult(entity, converted, id, cas,txResultHolder, null);
	}

	@Override
	public <T> T applyResult(T entity, CouchbaseDocument converted, Object id, long cas,
							 TransactionResultHolder txResultHolder, CouchbaseResourceHolder holder) {
		return applyResultBase(entity, converted, id, cas, txResultHolder, holder);
	}

	@Override
	public <T> Integer getTxResultHolder(T source) {
		return null;
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
	 * <p>
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
