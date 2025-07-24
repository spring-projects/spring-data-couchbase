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

package org.springframework.data.couchbase.core;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.index.CouchbasePersistentEntityIndexCreator;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.mapping.context.MappingContext;
import org.jspecify.annotations.Nullable;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Mono;

/**
 * Implements lower-level couchbase operations on top of the SDK with entity mapping capabilities.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jorge Rodriguez Martin
 * @author Tigran Babloyan
 * @since 3.0
 */
public class CouchbaseTemplate implements CouchbaseOperations, ApplicationContextAware {

	private final CouchbaseClientFactory clientFactory;
	private final CouchbaseConverter converter;
	private final CouchbaseTemplateSupport templateSupport;
	private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
	private final ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;
	private final QueryScanConsistency scanConsistency;
	private @Nullable CouchbasePersistentEntityIndexCreator indexCreator;

	public CouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter) {
		this(clientFactory, converter, new JacksonTranslationService());
	}

	public CouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter,
			final TranslationService translationService) {
		this(clientFactory, converter, translationService, null);
	}

	public CouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter,
			final TranslationService translationService, QueryScanConsistency scanConsistency) {
		this.clientFactory = clientFactory;
		this.converter = converter;
		this.templateSupport = new CouchbaseTemplateSupport(this, converter, translationService);
		this.reactiveCouchbaseTemplate = new ReactiveCouchbaseTemplate(clientFactory, converter, translationService,
				scanConsistency);
		this.scanConsistency = scanConsistency;

		this.mappingContext = this.converter.getMappingContext();
		if (mappingContext instanceof CouchbaseMappingContext) {
			CouchbaseMappingContext cmc = (CouchbaseMappingContext) mappingContext;
			if (cmc.isAutoIndexCreation()) {
				indexCreator = new CouchbasePersistentEntityIndexCreator(cmc, this);
			}
		}
	}

	@Override
	public <T> T save(T entity, String... scopeAndCollection) {
			Assert.notNull(entity, "Entity must not be null!");

			String scope = scopeAndCollection.length > 0 ? scopeAndCollection[0] : null;
			String collection = scopeAndCollection.length > 1 ? scopeAndCollection[1] : null;
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
					return (T)TransactionalSupport.checkForTransactionInThreadLocalStorage()
							.map(ctx -> {
								if (ctx.isPresent()) {
									return (T) insertById(clazz).inScope(scope)
											.inCollection(collection)
											.one(entity);
								} else { // if not in a tx, then upsert will work
									return (T) upsertById(clazz).inScope(scope)
											.inCollection(collection)
											.one(entity);
								}
							}).block();

				} else if (existingDocument) { // there is a version property, and it is non-zero
					// Updating existing document with cas
					return (T)replaceById(clazz).inScope(scope)
							.inCollection(collection)
							.one(entity);
				} else { // there is a version property, but it's zero or not set.
					// Creating new document
					return (T)insertById(clazz).inScope(scope)
							.inCollection(collection)
							.one(entity);
				}
		}

	@Override
	public <T> Long count(Query query, Class<T> domainType) {
		return findByQuery(domainType).matching(query).count();
	}

	@Override
	public <T> ExecutableUpsertById<T> upsertById(final Class<T> domainType) {
		return new ExecutableUpsertByIdOperationSupport(this).upsertById(domainType);
	}

	@Override
	public <T> ExecutableMutateInById<T> mutateInById(Class<T> domainType) {
		return new ExecutableMutateInByIdOperationSupport(this).mutateInById(domainType);
	}

	@Override
	public <T> ExecutableInsertById<T> insertById(Class<T> domainType) {
		return new ExecutableInsertByIdOperationSupport(this).insertById(domainType);
	}

	@Override
	public <T> ExecutableReplaceById<T> replaceById(Class<T> domainType) {
		return new ExecutableReplaceByIdOperationSupport(this).replaceById(domainType);
	}

	@Override
	public <T> ExecutableFindById<T> findById(Class<T> domainType) {
		return new ExecutableFindByIdOperationSupport(this).findById(domainType);
	}

	@Override
	public <T> ExecutableFindFromReplicasById<T> findFromReplicasById(Class<T> domainType) {
		return new ExecutableFindFromReplicasByIdOperationSupport(this).findFromReplicasById(domainType);
	}

	@Override
	public <T> ExecutableFindByQuery<T> findByQuery(Class<T> domainType) {
		return new ExecutableFindByQueryOperationSupport(this).findByQuery(domainType);
	}

	@Override
	public <T> ExecutableFindByAnalytics<T> findByAnalytics(Class<T> domainType) {
		return new ExecutableFindByAnalyticsOperationSupport(this).findByAnalytics(domainType);
	}

	@Override
	@Deprecated
	public ExecutableRemoveById removeById() {
		return removeById(null);
	}

	@Override
	public ExecutableRemoveById removeById(Class<?> domainType) {
		return new ExecutableRemoveByIdOperationSupport(this).removeById(domainType);
	}

	@Override
	@Deprecated
	public ExecutableExistsById existsById() {
		return existsById(null);
	}

	@Override
	public ExecutableExistsById existsById(Class<?> domainType) {
		return new ExecutableExistsByIdOperationSupport(this).existsById(domainType);
	}

	@Override
	public <T> ExecutableRemoveByQuery<T> removeByQuery(Class<T> domainType) {
		return new ExecutableRemoveByQueryOperationSupport(this).removeByQuery(domainType);
	}

	@Override
	public <T> ExecutableRangeScan<T> rangeScan(Class<T> domainType) {
		return new ExecutableRangeScanOperationSupport(this).rangeScan(domainType);
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

	@Override
	public QueryScanConsistency getConsistency() {
		return scanConsistency;
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

	public ReactiveCouchbaseTemplate reactive() {
		return reactiveCouchbaseTemplate;
	}

	@Override
	public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
		prepareIndexCreator(applicationContext);
		templateSupport.setApplicationContext(applicationContext);
		reactiveCouchbaseTemplate.setApplicationContext(applicationContext);
	}

	private void prepareIndexCreator(final ApplicationContext context) {
		String[] indexCreators = context.getBeanNamesForType(CouchbasePersistentEntityIndexCreator.class);

		for (String creator : indexCreators) {
			CouchbasePersistentEntityIndexCreator creatorBean = context.getBean(creator,
					CouchbasePersistentEntityIndexCreator.class);
			if (creatorBean.isIndexCreatorFor(mappingContext)) {
				return;
			}
		}

		if (context instanceof ConfigurableApplicationContext && indexCreator != null) {
			((ConfigurableApplicationContext) context).addApplicationListener(indexCreator);
			if (mappingContext instanceof CouchbaseMappingContext) {
				CouchbaseMappingContext cmc = (CouchbaseMappingContext) mappingContext;
				cmc.setIndexCreator(indexCreator);
			}
		}
	}

	public TemplateSupport support() {
		return templateSupport;
	}
}
