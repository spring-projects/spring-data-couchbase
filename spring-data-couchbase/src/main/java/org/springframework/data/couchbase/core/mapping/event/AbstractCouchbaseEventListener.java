/*
 * Copyright 2012-2020 the original author or authors
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
import org.springframework.context.ApplicationListener;
import org.springframework.core.GenericTypeResolver;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;

/**
 * Base class to implement domain class specific {@link ApplicationListener}s.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Martin Baumgartner
 * @author Michael Nitschinger
 */
public class AbstractCouchbaseEventListener<E> implements ApplicationListener<CouchbaseMappingEvent<?>> {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractCouchbaseEventListener.class);
	private final Class<?> domainClass;

	public AbstractCouchbaseEventListener() {
		Class<?> typeArgument = GenericTypeResolver.resolveTypeArgument(getClass(), AbstractCouchbaseEventListener.class);
		domainClass = typeArgument == null ? Object.class : typeArgument;
	}

	@SuppressWarnings("rawtypes")
	public void onApplicationEvent(CouchbaseMappingEvent<?> event) {

		E source = (E) event.getSource();
		// Check for matching domain type and invoke callbacks
		if (source != null && !domainClass.isAssignableFrom(source.getClass())) {
			return;
		}

		if (event instanceof BeforeDeleteEvent) {
			onBeforeDelete(event.getSource(), event.getDocument());
		} else if (event instanceof AfterDeleteEvent) {
			onAfterDelete(event.getSource(), event.getDocument());
		} else if (event instanceof BeforeConvertEvent) {
			onBeforeConvert(source);
		} else if (event instanceof BeforeSaveEvent) {
			onBeforeSave(source, event.getDocument());
		} else if (event instanceof AfterSaveEvent) {
			onAfterSave(source, event.getDocument());
		}
	}

	public void onBeforeConvert(E source) {
		if (LOG.isTraceEnabled()) {
			LOG.trace("onBeforeConvert({})", source);
		}
	}

	public void onBeforeSave(E source, CouchbaseDocument doc) {
		if (LOG.isTraceEnabled()) {
			LOG.trace("onBeforeSave({}, {})", source, doc);
		}
	}

	public void onAfterSave(E source, CouchbaseDocument doc) {
		if (LOG.isTraceEnabled()) {
			LOG.trace("onAfterSave({}, {})", source, doc);
		}
	}

	public void onAfterDelete(Object source, CouchbaseDocument doc) {
		if (LOG.isTraceEnabled()) {
			LOG.trace("onAfterConvert({})", doc);
		}
	}

	public void onBeforeDelete(Object source, CouchbaseDocument doc) {
		if (LOG.isTraceEnabled()) {
			LOG.trace("onAfterConvert({})", doc);
		}
	}

}
