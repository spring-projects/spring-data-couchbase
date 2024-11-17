/*
 * Copyright 2012-2024 the original author or authors
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
import org.springframework.data.couchbase.core.mapping.event.*;
import org.springframework.data.couchbase.transaction.CouchbaseResourceHolder;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.util.Assert;

/**
 * Internal encode/decode support for CouchbaseTemplate.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jorge Rodriguez Martin
 * @author Carlos Espinaco
 * @author Mico Piira
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
	public <T> EncodedEntity<T> encodeEntity(final T entityToEncode) {
		maybeEmitEvent(new BeforeConvertEvent<>(entityToEncode));
		Object maybeNewEntity = maybeCallBeforeConvert(entityToEncode, "");
		final CouchbaseDocument converted = new CouchbaseDocument();
		converter.write(maybeNewEntity, converted);
		maybeEmitEvent(new BeforeSaveEvent<>(entityToEncode, converted));
		return new EncodedEntity<>(maybeCallBeforeSave(entityToEncode, converted, ""), converted);
	}

	@Override
	public <T> T decodeEntity(Object id, String source, Long cas, Class<T> entityClass, String scope, String collection,
			Object txHolder, CouchbaseResourceHolder holder) {
		CouchbaseDocument converted = new CouchbaseDocument(id);
		T decoded = decodeEntityBase(id, source, cas, entityClass, scope, collection, txHolder, holder, converted);
		maybeEmitEvent(new AfterConvertEvent<>(decoded, converted));
		return maybeCallAfterConvert(decoded, converted, "");
	}

	@Override
	public <T> T applyResult(T entity, CouchbaseDocument converted, Object id, long cas,
			Object txResultHolder, CouchbaseResourceHolder holder) {
		T applied = applyResultBase(entity, id, cas, txResultHolder, holder);
		maybeEmitEvent(new AfterSaveEvent<>(applied, converted));
		return maybeCallAfterSave(applied, converted, "");
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

	protected <T> T maybeCallAfterSave(T object, CouchbaseDocument document, String collection) {
		if (null != entityCallbacks) {
			return entityCallbacks.callback(AfterSaveCallback.class, object, document, collection);
		} else {
			LOG.info("maybeCallAfterSave called, but CouchbaseTemplate not initialized with applicationContext");
		}
		return object;
	}

	protected <T> T maybeCallBeforeSave(T object, CouchbaseDocument document, String collection) {
		if (null != entityCallbacks) {
			return entityCallbacks.callback(BeforeSaveCallback.class, object, document, collection);
		} else {
			LOG.info("maybeCallBeforeSave called, but CouchbaseTemplate not initialized with applicationContext");
		}
		return object;
	}

	@Override
	ReactiveCouchbaseTemplate getReactiveTemplate() {
		return template.reactive();
	}
}
