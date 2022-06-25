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
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import org.springframework.data.couchbase.transaction.CouchbaseResourceHolder;
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
	public <T> T decodeEntity(String id, String source, Long cas, Class<T> entityClass, String scope, String collection,
							  TransactionResultHolder txHolder) {
		return decodeEntity(id, source, cas, entityClass, scope, collection, txHolder);
	}

	@Override
	public <T> T decodeEntity(String id, String source, Long cas, Class<T> entityClass, String scope, String collection,
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

	@Override
	ReactiveCouchbaseTemplate getReactiveTemplate() {
		return template.reactive();
	}
}
