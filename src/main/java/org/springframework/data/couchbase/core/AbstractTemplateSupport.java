/*
 * Copyright 2022-present the original author or authors
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
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

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
import org.springframework.data.couchbase.core.support.TemplateUtils;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;
import org.springframework.data.couchbase.transaction.CouchbaseResourceHolder;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.util.ClassUtils;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;

/**
 * Base shared by Reactive and non-Reactive TemplateSupport
 *
 * @author Michael Reiche
 * @author Emilien Bevierre
 */
@Stability.Internal
public abstract class AbstractTemplateSupport {

	final ReactiveCouchbaseTemplate template;
	final CouchbaseConverter converter;
	final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
	final TranslationService translationService;
	ApplicationContext applicationContext;
	static final Logger LOG = LoggerFactory.getLogger(AbstractTemplateSupport.class);

	public AbstractTemplateSupport(ReactiveCouchbaseTemplate template, CouchbaseConverter converter,
			TranslationService translationService) {
		this.template = template;
		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
		this.translationService = translationService;
	}

	abstract ReactiveCouchbaseTemplate getReactiveTemplate();

	public <T> T decodeEntityBase(Object id, String source, Long cas, Instant expiryTime, Class<T> entityClass,
			String scope, String collection, Object txResultHolder, CouchbaseResourceHolder holder) {
		return decodeEntityBase(id, cas, expiryTime, entityClass, scope, collection, txResultHolder, holder,
				(ts, converted) -> (CouchbaseDocument) ts.decode(source, converted));
	}

	public <T> T decodeEntityBase(Object id, byte[] source, Long cas, Instant expiryTime, Class<T> entityClass,
			String scope, String collection, Object txResultHolder, CouchbaseResourceHolder holder) {
		return decodeEntityBase(id, cas, expiryTime, entityClass, scope, collection, txResultHolder, holder,
				(ts, converted) -> (CouchbaseDocument) ts.decode(source, converted));
	}

	private <T> T decodeEntityBase(Object id, Long cas, Instant expiryTime, Class<T> entityClass, String scope,
			String collection, Object txResultHolder, CouchbaseResourceHolder holder,
			BiFunction<TranslationService, CouchbaseDocument, CouchbaseDocument> translatorFn) {
		CouchbasePersistentEntity persistentEntity = couldBePersistentEntity(entityClass);

		if (persistentEntity == null) {
			final CouchbaseDocument converted = new CouchbaseDocument(id);
			Set<Map.Entry<String, Object>> set = translatorFn.apply(translationService, converted).getContent()
					.entrySet();
			return (T) set.iterator().next().getValue();
		}

		final CouchbaseDocument converted = prepareConvertedDocument(id, cas, persistentEntity);
		T readEntity = converter.read(entityClass, translatorFn.apply(translationService, converted));
		return finalizeEntity(readEntity, id, cas, expiryTime, scope, collection, txResultHolder, holder);
	}

	private CouchbaseDocument prepareConvertedDocument(Object id, Long cas,
			CouchbasePersistentEntity persistentEntity) {
		// persistentEntity is derived from the entityClass declared in the
		// repository definition. It may be an abstract class rather than the
		// concrete class of the document being read. The concrete type is only
		// known after converter.read() is called, therefore version/cas is set again
		// on the final entity in finalizeEntity().
		//
		// Pre-populating the CAS/version into the source document (done in getDocument)
		// is a best-effort step to avoid a construction failure when the concrete
		// class constructor takes a primitive "long version" argument (null is not a
		// valid value for a primitive). If the version property is only declared on the
		// concrete subclass and not on the abstract base, pre-population is not
		// possible here and the issue can be avoided by using "Long" instead of "long".
		//
		// An alternative would be to resolve the actual concrete type from the source
		// document's type metadata before constructing it (see the comment
		// below), but that adds overhead for every decode to solve a rare and avoidable
		// case.
		// TypeInformation<? extends R> typeToUse = typeMapper.readType(source, type);

		if (id == null) {
			throw new CouchbaseException(
					TemplateUtils.SELECT_ID + " was null. Either use #{#n1ql.selectEntity} or project "
							+ TemplateUtils.SELECT_ID);
		}

        return getDocument(id, cas, persistentEntity);
	}

    private static CouchbaseDocument getDocument(Object id, Long cas, CouchbasePersistentEntity persistentEntity) {
        final CouchbaseDocument converted = new CouchbaseDocument(id);

        // If possible, set the version property in the source so that if the
        // constructor has a long version argument, it will have a value and succeed,
        // as null is not a valid argument for a long. This failure can be avoided by
        // defining the argument as Long instead of long.
        // Note that persistentEntity is still the (possibly abstract) class specified
        // in the repository definition, so it's possible that the abstract class does
        // not have a version property, in which case this won't be able to set the version.
        if (persistentEntity.getVersionProperty() != null) {
            if (cas == null) {
                throw new CouchbaseException("version/cas in the entity but " + TemplateUtils.SELECT_CAS
                        + " was not in result. Either use #{#n1ql.selectEntity} or project "
                        + TemplateUtils.SELECT_CAS);
            }
            if (cas != 0) {
                converted.put(persistentEntity.getVersionProperty().getName(), cas);
            }
        }
        return converted;
    }

    private <T> T finalizeEntity(T readEntity, Object id, Long cas, Instant expiryTime, String scope, String collection,
			Object txResultHolder, CouchbaseResourceHolder holder) {
		final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(readEntity);

		CouchbasePersistentEntity persistentEntity = couldBePersistentEntity(readEntity.getClass());

		// If the constructor has a long version argument, construction will fail if
		// 'version' is not available, as null is not a legal value for a long.
		// We therefore use the object-wrapped Long type.
		// (Version doesn't come from 'source', it comes from the cas argument to
		// decodeEntity.)
		if (cas != null && cas != 0 && persistentEntity.getVersionProperty() != null) {
			accessor.setProperty(persistentEntity.getVersionProperty(), cas);
		}

		if (expiryTime != null && persistentEntity.getExpiryProperty() != null) {
			accessor.setProperty(persistentEntity.getExpiryProperty(), expiryTime);
		}

		N1qlJoinResolver.handleProperties(persistentEntity, accessor, getReactiveTemplate(), id.toString(), scope,
				collection);

		if (holder != null) {
			holder.transactionResultHolder(txResultHolder, (T) accessor.getBean());
		}

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

	public <T> T applyResultBase(T entity, CouchbaseDocument converted, Object id, long cas,
			Object txResultHolder, CouchbaseResourceHolder holder) {
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

		if (holder != null) {
			holder.transactionResultHolder(txResultHolder, (T) accessor.getBean());
		}
		maybeEmitEvent(new AfterSaveEvent(accessor.getBean(), converted));
		return (T) accessor.getBean();

	}

	public Long getCas(final Object entity) {
		final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext
				.getRequiredPersistentEntity(entity.getClass());
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
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext
				.getRequiredPersistentEntity(entity.getClass());
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

	public TranslationService getTranslationService() {
		return translationService;
	}
}
