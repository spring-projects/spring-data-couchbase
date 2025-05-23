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

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.data.auditing.AuditingHandler;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

/**
 * Event listener to populate auditing related fields on an entity about to be saved.
 *
 * @author Oliver Gierke
 * @author Simon Baslé
 * @author Mark Paluch
 * @author Michael Reiche
 */
public class AuditingEventListener implements ApplicationListener<CouchbaseMappingEvent<?>> {

	private final ObjectFactory<Object> auditingHandlerFactory;

	public AuditingEventListener() {
		this.auditingHandlerFactory = null;
	}

	private static final Logger LOG = LoggerFactory.getLogger(AuditingEventListener.class);

	/**
	 * Creates a new {@link AuditingEventListener} using the given {@link MappingContext} and {@link AuditingHandler}
	 * provided by the given {@link ObjectFactory}. Registered in CouchbaseAuditingRegistrar
	 *
	 * @param auditingHandlerFactory must not be {@literal null}.
	 */
	public AuditingEventListener(ObjectFactory<Object> auditingHandlerFactory) {
		Assert.notNull(auditingHandlerFactory, "auditingHandlerFactory must not be null!");
		this.auditingHandlerFactory = auditingHandlerFactory;
		Object o = auditingHandlerFactory.getObject();
		if(!(o instanceof IsNewAwareAuditingHandler)){
			LOG.warn("auditingHandler IS NOT an IsNewAwareAuditingHandler: {}",o);
		} else {
			LOG.info("auditingHandler IS an IsNewAwareAuditingHandler: {}",o);
		}
	}

	/**
	 * (non-Javadoc)
	 * @see {@link org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)}
	 */
	@Override
	public void onApplicationEvent(CouchbaseMappingEvent<?> event) {
		if (event instanceof BeforeConvertEvent) {
			IsNewAwareAuditingHandler h = auditingHandlerFactory != null
					&& auditingHandlerFactory.getObject() instanceof IsNewAwareAuditingHandler
							? (IsNewAwareAuditingHandler) (auditingHandlerFactory.getObject())
							: null;
			if (auditingHandlerFactory != null && h == null) {
				if (LOG.isWarnEnabled()) {
					LOG.warn("event:{} source:{} auditingHandler is not an IsNewAwareAuditingHandler: {}",
							event.getClass().getSimpleName(), event.getSource(), auditingHandlerFactory.getObject());
				}
			}
			if (h != null) {
				Optional.ofNullable(event.getSource()).ifPresent(it -> h.markAudited(it));
			}
		}
		if (event instanceof BeforeSaveEvent) {}
		if (event instanceof AfterSaveEvent) {}
		if (event instanceof BeforeDeleteEvent) {}
		if (event instanceof AfterDeleteEvent) {}

		if (LOG.isTraceEnabled()) {
			LOG.trace("{} {}", event.getClass().getSimpleName(), event.getSource());
		}
	}

}
