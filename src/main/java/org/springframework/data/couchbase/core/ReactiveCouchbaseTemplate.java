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

package org.springframework.data.couchbase.core;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.support.PseudoArgs;

import com.couchbase.client.java.Collection;

/**
 * template class for Reactive Couchbase operations
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jorge Rodriguez Martin
 */
public class ReactiveCouchbaseTemplate implements ReactiveCouchbaseOperations, ApplicationContextAware {

	private final CouchbaseClientFactory clientFactory;
	private final CouchbaseConverter converter;
	private final PersistenceExceptionTranslator exceptionTranslator;
	private final CouchbaseTemplateSupport templateSupport;
	private ThreadLocal<PseudoArgs<?>> thrdLocalArgs = new ThreadLocal<>();

	public ReactiveCouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter) {
		this(clientFactory, converter, new JacksonTranslationService());
	}

	public ReactiveCouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter,
			final TranslationService translationService) {
		this.clientFactory = clientFactory;
		this.converter = converter;
		this.exceptionTranslator = clientFactory.getExceptionTranslator();
		this.templateSupport = new CouchbaseTemplateSupport(converter, translationService);
	}

	@Override
	public <T> ReactiveFindById<T> findById(Class<T> domainType) {
		return new ReactiveFindByIdOperationSupport(this).findById(domainType);
	}

	@Override
	public ReactiveExistsById existsById() {
		return new ReactiveExistsByIdOperationSupport(this).existsById();
	}

	@Override
	public <T> ReactiveFindByAnalytics<T> findByAnalytics(Class<T> domainType) {
		return new ReactiveFindByAnalyticsOperationSupport(this).findByAnalytics(domainType);
	}

	@Override
	public <T> ReactiveFindByQuery<T> findByQuery(Class<T> domainType) {
		return new ReactiveFindByQueryOperationSupport(this).findByQuery(domainType);
	}

	@Override
	public <T> ReactiveFindFromReplicasById<T> findFromReplicasById(Class<T> domainType) {
		return new ReactiveFindFromReplicasByIdOperationSupport(this).findFromReplicasById(domainType);
	}

	@Override
	public <T> ReactiveInsertById<T> insertById(Class<T> domainType) {
		return new ReactiveInsertByIdOperationSupport(this).insertById(domainType);
	}

	@Override
	public ReactiveRemoveById removeById() {
		return new ReactiveRemoveByIdOperationSupport(this).removeById();
	}

	@Override
	public <T> ReactiveRemoveByQuery<T> removeByQuery(Class<T> domainType) {
		return new ReactiveRemoveByQueryOperationSupport(this).removeByQuery(domainType);
	}

	@Override
	public <T> ReactiveReplaceById<T> replaceById(Class<T> domainType) {
		return new ReactiveReplaceByIdOperationSupport(this).replaceById(domainType);
	}

	@Override
	public <T> ReactiveUpsertById<T> upsertById(Class<T> domainType) {
		return new ReactiveUpsertByIdOperationSupport(this).upsertById(domainType);
	}

	@Override
	public String getBucketName() {
		return clientFactory.getBucket().name();
	}

	@Override
	public String getScopeName() {
		return clientFactory.getScope().name();
	}

	@Override
	public CouchbaseClientFactory getCouchbaseClientFactory() {
		return clientFactory;
	}

	/**
	 * Provides access to a {@link Collection} on the configured {@link CouchbaseClientFactory}.
	 *
	 * @param collectionName the name of the collection, if null is passed in the default collection is assumed.
	 * @return the collection instance.
	 */
	public Collection getCollection(final String collectionName) {
		return clientFactory.getCollection(collectionName);
	}

	@Override
	public CouchbaseConverter getConverter() {
		return converter;
	}

	CouchbaseTemplateSupport support() {
		return templateSupport;
	}

	/**
	 * Tries to convert the given {@link RuntimeException} into a {@link DataAccessException} but returns the original
	 * exception if the conversation failed. Thus allows safe re-throwing of the return value.
	 *
	 * @param ex the exception to translate
	 */
	RuntimeException potentiallyConvertRuntimeException(final RuntimeException ex) {
		RuntimeException resolved = exceptionTranslator.translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}

	@Override
	public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
		templateSupport.setApplicationContext(applicationContext);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public PseudoArgs<?> getThreadLocalArgs() {
		return thrdLocalArgs == null ? null : thrdLocalArgs.get();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setThreadLocalArgs(PseudoArgs<?> pseudoArgs) {
		this.thrdLocalArgs.set(pseudoArgs);
	}

}
