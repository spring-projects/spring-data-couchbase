/*
 * Copyright 2012-2025 the original author or authors
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
package org.springframework.data.couchbase.core.mapping.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.core.Ordered;
import org.springframework.data.auditing.AuditingHandler;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

/**
 * {@link EntityCallback} to populate auditing related fields on an entity about to be saved.
 *
 * @author Jorge Rodríguez Martín
 * @since 4.2
 */
public class AuditingEntityCallback implements BeforeConvertCallback<Object>, AfterConvertCallback<Object>, Ordered {

	private final ObjectFactory<IsNewAwareAuditingHandler> auditingHandlerFactory;
	private static final Logger LOG = LoggerFactory.getLogger(AuditingEntityCallback.class);

	/**
	 * Creates a new {@link AuditingEntityCallback} using the given {@link MappingContext} and {@link AuditingHandler}
	 * provided by the given {@link ObjectFactory}.
	 *
	 * @param auditingHandlerFactory must not be {@literal null}.
	 */
	public AuditingEntityCallback(ObjectFactory<IsNewAwareAuditingHandler> auditingHandlerFactory) {
		Assert.notNull(auditingHandlerFactory, "IsNewAwareAuditingHandler must not be null!");
		this.auditingHandlerFactory = auditingHandlerFactory;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.core.mapping.event.BeforeConvertCallback#onBeforeConvert(java.lang.Object, java.lang.String)
	 */
	@Override
	public Object onBeforeConvert(Object entity, String collection) {
		// LOG.trace("onBeforeConvert " + entity);
		return entity; // markAudited called in AuditingEventListener.onApplicationEvent()
										// auditingHandlerFactory.getObject().markAudited(entity);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.core.mapping.event.AfterConvertCallback#onAfterConvert(java.lang.Object, CouchbaseDocument, java.lang.String)
	 */
	@Override
	public Object onAfterConvert(Object entity, CouchbaseDocument document, String collection) {
		// LOG.trace("onAfterConvert " + document);
		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.Ordered#getOrder()
	 */
	@Override
	public int getOrder() {
		return 100;
	}

}
