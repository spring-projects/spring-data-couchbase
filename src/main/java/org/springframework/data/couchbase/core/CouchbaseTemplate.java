/*
 * Copyright 2012-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.couchbase.client.java.Bucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbaseStorable;

/**
 * @author Michael Nitschinger
 * @author Oliver Gierke
 * @author Simon Baslé
 */
public class CouchbaseTemplate implements CouchbaseOperations, ApplicationEventPublisherAware {

	private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTemplate.class);
	private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;
	private static final Collection<String> ITERABLE_CLASSES;
	static {
		final Set<String> iterableClasses = new HashSet<String>();
		iterableClasses.add(List.class.getName());
		iterableClasses.add(Collection.class.getName());
		iterableClasses.add(Iterator.class.getName());
		ITERABLE_CLASSES = Collections.unmodifiableCollection(iterableClasses);
	}

	private final Bucket client;
	private final CouchbaseConverter converter;
	private final TranslationService translationService;


	private ApplicationEventPublisher eventPublisher;
	private WriteResultChecking writeResultChecking = DEFAULT_WRITE_RESULT_CHECKING;

	public CouchbaseTemplate(final Bucket client) {
		this(client, null, null);
	}

	public CouchbaseTemplate(final Bucket client, final CouchbaseConverter converter) {
		this(client, converter, null);
	}

	public CouchbaseTemplate(final Bucket client, final CouchbaseConverter converter,
							 final TranslationService translationService) {
		this.client = client;
		this.converter = converter == null ? getDefaultConverter() : converter;
		this.translationService = translationService == null ? getDefaultTranslationService() : translationService;
	}

	private TranslationService getDefaultTranslationService() {
		JacksonTranslationService t = new JacksonTranslationService();
		t.afterPropertiesSet();
		return t;
	}

	private CouchbaseConverter getDefaultConverter() {
		MappingCouchbaseConverter c = new MappingCouchbaseConverter(new CouchbaseMappingContext());
		c.afterPropertiesSet();
		return c;
	}

	/** Encode a {@link CouchbaseStorable} into a storable representation (JSON) **/
	private Object translateEncode(final CouchbaseStorable source) {
		return translationService.encode(source);
	}


	/** Decode a JSON string into a {@link CouchbaseStorable} **/
	private CouchbaseStorable translateDecode(final String source, final CouchbaseStorable target) {
		return translationService.decode(source, target);
	}

	/**
	 * Make sure the given object is not a iterable.
	 *
	 * @param o the object to verify.
	 */
	protected static void ensureNotIterable(Object o) {
		if (null != o) {
			if (o.getClass().isArray() || ITERABLE_CLASSES.contains(o.getClass().getName())) {
				throw new IllegalArgumentException("Cannot use a collection here.");
			}
		}
	}

	/**
	 * Handle write errors according to the set {@link #writeResultChecking} setting.
	 *
	 * @param message the message to use.
	 */
	private void handleWriteResultError(String message) {
		if (writeResultChecking == WriteResultChecking.NONE) {
			return;
		}

		if (writeResultChecking == WriteResultChecking.EXCEPTION) {
			throw new CouchbaseDataIntegrityViolationException(message);
		} else {
			LOGGER.error(message);
		}
	}

	public void setWriteResultChecking(WriteResultChecking writeResultChecking) {
		this.writeResultChecking = writeResultChecking == null ? DEFAULT_WRITE_RESULT_CHECKING : writeResultChecking;
	}

	@Override
	public void setApplicationEventPublisher(final ApplicationEventPublisher eventPublisher) {
		this.eventPublisher = eventPublisher;
	}

	@Override
	public Bucket getCouchbaseBucket() {
		return this.client;
	}

	@Override
	public CouchbaseConverter getConverter() {
		return this.converter;
	}
}
