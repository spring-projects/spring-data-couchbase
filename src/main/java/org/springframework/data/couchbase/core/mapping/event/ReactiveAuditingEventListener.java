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

package org.springframework.data.couchbase.core.mapping.event;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.data.auditing.AuditingHandler;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.auditing.ReactiveIsNewAwareAuditingHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

/**
 * Reactive Event listener to populate auditing related fields on an entity about to be saved.
 *
 * @author Michael Reiche
 */
public class ReactiveAuditingEventListener implements ApplicationListener<CouchbaseMappingEvent<Object>> {

	private final ObjectFactory<ReactiveIsNewAwareAuditingHandler> auditingHandlerFactory;

	public ReactiveAuditingEventListener() {
		this.auditingHandlerFactory = null;
	}

	private static final Logger LOG = LoggerFactory.getLogger(ReactiveAuditingEventListener.class);

	/**
	 * Creates a new {@link ReactiveAuditingEventListener} using the given {@link MappingContext} and
	 * {@link AuditingHandler} provided by the given {@link ObjectFactory}. Registered in CouchbaseAuditingRegistrar
	 *
	 * @param auditingHandlerFactory must not be {@literal null}.
	 */
	public ReactiveAuditingEventListener(ObjectFactory<ReactiveIsNewAwareAuditingHandler> auditingHandlerFactory) {
		Assert.notNull(auditingHandlerFactory, "IsNewAwareAuditingHandler must not be null!");
		this.auditingHandlerFactory = auditingHandlerFactory;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
	 */
	@Override
	public void onApplicationEvent(CouchbaseMappingEvent<Object> event) {
		if (event instanceof ReactiveBeforeConvertEvent) {
			Optional.ofNullable(event.getSource())//
					.ifPresent(it -> auditingHandlerFactory.getObject().markAudited(it));
			// LOG.info(event.getClass().getSimpleName() + " " + event);
		}
		if (event instanceof ReactiveBeforeSaveEvent) {
			// LOG.info(event.getClass().getSimpleName() + " " + event);
		}
		if (event instanceof ReactiveAfterSaveEvent) {
			// LOG.info(event.getClass().getSimpleName() + " " + event);
		}
		if (event instanceof ReactiveBeforeDeleteEvent) {
			// LOG.info(event.getClass().getSimpleName() + " " + event);
		}
		if (event instanceof ReactiveAfterDeleteEvent) {
			// LOG.info(event.getClass().getSimpleName() + " " + event);
		}
		if (event.getClass().getSimpleName().startsWith("Reactive")) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(event.getClass().getSimpleName() + " " + event.getSource());
			}
		}
	}

}
