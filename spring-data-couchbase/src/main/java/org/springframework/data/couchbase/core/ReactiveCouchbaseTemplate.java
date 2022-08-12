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

import reactor.core.publisher.Mono;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * template class for Reactive Couchbase operations
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jorge Rodriguez Martin
 * @author Carlos Espinaco
 */
public class ReactiveCouchbaseTemplate implements ReactiveCouchbaseOperations, ApplicationContextAware {

	private final CouchbaseClientFactory clientFactory;
	private final CouchbaseConverter converter;
	private final PersistenceExceptionTranslator exceptionTranslator;
	private final ReactiveCouchbaseTemplateSupport templateSupport;
	private ThreadLocal<PseudoArgs<?>> threadLocalArgs = new ThreadLocal<>();
	private final QueryScanConsistency scanConsistency;

	public ReactiveCouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter) {
		this(clientFactory, converter, new JacksonTranslationService(), null);
	}

	public ReactiveCouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter,
			final TranslationService translationService) {
		this(clientFactory, converter, translationService, null);
	}

	public ReactiveCouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter,
			final TranslationService translationService, final QueryScanConsistency scanConsistency) {
		this.clientFactory = clientFactory;
		this.converter = converter;
		this.exceptionTranslator = clientFactory.getExceptionTranslator();
		this.templateSupport = new ReactiveCouchbaseTemplateSupport(this, converter, translationService);
		this.scanConsistency = scanConsistency;
	}

	@Override
	public <T> Mono<T> save(T entity, String... scopeAndCollection) {
		Assert.notNull(entity, "Entity must not be null!");
		
		String scope = scopeAndCollection.length > 0 ? scopeAndCollection[0] : null;
		String collection = scopeAndCollection.length > 1 ? scopeAndCollection[1] : null;
		return Mono.defer(() -> {
			final CouchbasePersistentEntity<?> mapperEntity = getConverter().getMappingContext()
					.getPersistentEntity(entity.getClass());
			final CouchbasePersistentProperty versionProperty = mapperEntity.getVersionProperty();
			final boolean versionPresent = versionProperty != null;
			final Long version = versionProperty == null || versionProperty.getField() == null ? null
					: (Long) ReflectionUtils.getField(versionProperty.getField(),
					entity);
			final boolean existingDocument = version != null && version > 0;

			Class clazz = entity.getClass();

			if (!versionPresent) { // the entity doesn't have a version property
				// No version field - no cas
				// If in a transaction, insert is the only thing that will work
				return TransactionalSupport.checkForTransactionInThreadLocalStorage()
						.flatMap(ctx -> {
							if (ctx.isPresent()) {
								return (Mono<T>) insertById(clazz).inScope(scope)
										.inCollection(collection)
										.one(entity);
							} else { // if not in a tx, then upsert will work
								return (Mono<T>) upsertById(clazz).inScope(scope)
										.inCollection(collection)
										.one(entity);
							}
						});
			} else if (existingDocument) { // there is a version property, and it is non-zero
				// Updating existing document with cas
				return (Mono<T>) replaceById(clazz).inScope(scope)
						.inCollection(collection)
						.one(entity);
			} else { // there is a version property, but it's zero or not set.
				// Creating new document
				return (Mono<T>) insertById(clazz).inScope(scope)
						.inCollection(collection)
						.one(entity);
			}
		});
	}

	public <T> Mono<Long> count(Query query, Class<T> domainType) {
		return findByQuery(domainType).matching(query).all().count();
	}

	@Override
	public <T> ReactiveFindById<T> findById(Class<T> domainType) {
		return new ReactiveFindByIdOperationSupport(this).findById(domainType);
	}

	@Override
	public ReactiveExistsById existsById() {
		return existsById(null);
	}

	@Override
	public ReactiveExistsById existsById(Class<?> domainType) {
		return new ReactiveExistsByIdOperationSupport(this).existsById(domainType);
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
		return removeById(null);
	}

	@Override
	public ReactiveRemoveById removeById(Class<?> domainType) {
		return new ReactiveRemoveByIdOperationSupport(this).removeById(domainType);
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

	public ReactiveTemplateSupport support() {
		return templateSupport;
	}

	/**
	 * Tries to convert the given {@link RuntimeException} into a {@link DataAccessException} but returns the original
	 * exception if the conversation failed. Thus allows safe re-throwing of the return value.
	 *
	 * @param ex the exception to translate
	 */
	RuntimeException potentiallyConvertRuntimeException(final RuntimeException ex) {
		RuntimeException resolved = exceptionTranslator != null ? exceptionTranslator.translateExceptionIfPossible(ex)
				: null;
		return resolved == null ? ex : resolved;
	}

	@Override
	public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
		templateSupport.setApplicationContext(applicationContext);
	}

	/**
	 * @return the pseudoArgs from the ThreadLocal field
	 */
	public PseudoArgs<?> getPseudoArgs() {
		return threadLocalArgs == null ? null : threadLocalArgs.get();
	}

	/**
	 * set the ThreadLocal field
	 */
	public void setPseudoArgs(PseudoArgs<?> threadLocalArgs) {
		this.threadLocalArgs = new ThreadLocal<>();
		this.threadLocalArgs.set(threadLocalArgs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public QueryScanConsistency getConsistency() {
		return scanConsistency;
	}

}
