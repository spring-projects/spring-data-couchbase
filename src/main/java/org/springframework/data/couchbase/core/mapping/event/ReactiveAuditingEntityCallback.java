/*
 * Copyright 2021-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core.mapping.event;

import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.core.Ordered;
import org.springframework.data.auditing.AuditingHandler;
import org.springframework.data.auditing.ReactiveIsNewAwareAuditingHandler;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

/**
 * Reactive {@link EntityCallback} to populate auditing related fields on an entity about to be saved. Based on <a href=
 * "https://github.com/spring-projects/spring-data-mongodb/blob/3.1.6/spring-data-mongodb/src/main/java/org/springframework/data/mongodb/core/mapping/event/ReactiveAfterConvertCallback.java">ReactiveAfterConvertCallback</a>
 *
 * @author Jorge Rodríguez Martín
 * @authoer Michael Reiche
 * @since 4.2
 */
public class ReactiveAuditingEntityCallback
		implements ReactiveBeforeConvertCallback<Object>, ReactiveAfterConvertCallback<Object>, Ordered {

	private final ObjectFactory<ReactiveIsNewAwareAuditingHandler> auditingHandlerFactory;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveAuditingEntityCallback.class);

	/**
	 * Creates a new {@link ReactiveAuditingEntityCallback} using the given {@link MappingContext} and
	 * {@link AuditingHandler} provided by the given {@link ObjectFactory}.
	 *
	 * @param auditingHandlerFactory must not be {@literal null}.
	 */
	public ReactiveAuditingEntityCallback(final ObjectFactory<ReactiveIsNewAwareAuditingHandler> auditingHandlerFactory) {
		Assert.notNull(auditingHandlerFactory, "ReactiveIsNewAwareAuditingHandler must not be null!");
		this.auditingHandlerFactory = auditingHandlerFactory;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.springframework.data.couchbase.core.mapping.event.ReactiveBeforeConvertCallback#onBeforeConvert
	 * (java.lang.Object, java.lang.String)
	 */
	@Override
	public Publisher<Object> onBeforeConvert(final Object entity, final String collection) {
		if (LOG.isTraceEnabled()) {
			LOG.trace("onBeforeConvert {}", entity.toString());
		}
		return this.auditingHandlerFactory.getObject().markAudited(entity);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.springframework.data.couchbase.core.mapping.event.ReactiveAfterConvertCallback#onAfterConvert
	 * (java.lang.Object, CouchbaseDocument, java.lang.String)
	 */
	@Override
	public Publisher<Object> onAfterConvert(Object entity, CouchbaseDocument document, String collection) {
		if (LOG.isTraceEnabled()) {
			LOG.trace("onAfterConvert {}", document.toString());
		}
		return Mono.just(entity);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.springframework.core.Ordered#getOrder()
	 */
	@Override
	public int getOrder() {
		return 100;
	}

}
