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

import static org.springframework.data.couchbase.repository.support.Util.hasNonZeroVersionProperty;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.index.CouchbasePersistentEntityIndexCreator;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Implements lower-level couchbase operations on top of the SDK with entity mapping capabilities.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jorge Rodriguez Martin
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
	private CouchbaseTransactionalOperator couchbaseTransactionalOperator;

	public CouchbaseTemplate(final CouchbaseClientFactory clientFactory,
			final ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory, final CouchbaseConverter converter) {
		this(clientFactory, reactiveCouchbaseClientFactory, converter, new JacksonTranslationService());
	}

	public CouchbaseTemplate(final CouchbaseClientFactory clientFactory,
			final ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory, CouchbaseConverter converter,
			final TranslationService translationService) {
		this(clientFactory, reactiveCouchbaseClientFactory, converter, translationService, null);
	}

	public CouchbaseTemplate(final CouchbaseClientFactory clientFactory,
			final ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory, final CouchbaseConverter converter,
			final TranslationService translationService, QueryScanConsistency scanConsistency) {
		this.clientFactory = clientFactory;
		this.converter = converter;
		this.templateSupport = new CouchbaseTemplateSupport(this, converter, translationService);
		this.reactiveCouchbaseTemplate = new ReactiveCouchbaseTemplate(reactiveCouchbaseClientFactory, converter,
				translationService, scanConsistency);
		this.scanConsistency = scanConsistency;

		this.mappingContext = this.converter.getMappingContext();
		if (mappingContext instanceof CouchbaseMappingContext) {
			CouchbaseMappingContext cmc = (CouchbaseMappingContext) mappingContext;
			if (cmc.isAutoIndexCreation()) {
				indexCreator = new CouchbasePersistentEntityIndexCreator(cmc, this);
			}
		}
	}

	public <T> T save(T entity) {
		if (hasNonZeroVersionProperty(entity, templateSupport.converter)) {
			return replaceById((Class<T>) entity.getClass()).one(entity);
		//} else if (getTransactionalOperator() != null) {
		//	return insertById((Class<T>) entity.getClass()).one(entity);
		} else {
			return upsertById((Class<T>) entity.getClass()).one(entity);
		}
	}

	public <T> Long count(Query query, Class<T> domainType) {
		return findByQuery(domainType).matching(query).count();
	}

	@Override
	public <T> ExecutableUpsertById<T> upsertById(final Class<T> domainType) {
		return new ExecutableUpsertByIdOperationSupport(this).upsertById(domainType);
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

	public CouchbaseTemplate with(CouchbaseTransactionalOperator couchbaseTransactionalOperator) {
		this.couchbaseTransactionalOperator = couchbaseTransactionalOperator;
		return this;
	}

	/**
	 * Get the TransactionalOperator from <br>
	 * 1. The template.clientFactory<br>
	 * 2. The template.threadLocal<br>
	 * 3. otherwise null<br>
	 * This can be overriden in the operation method by<br>
	 * 1. repository.withCollection()
	 *//*
	private CouchbaseStuffHandle getTransactionalOperator() {
		if (this.getCouchbaseClientFactory().getTransactionalOperator() != null) {
			return this.getCouchbaseClientFactory().getTransactionalOperator();
		}
		ReactiveCouchbaseTemplate t = this.reactive();
		PseudoArgs pArgs = t.getPseudoArgs();
		if (pArgs != null && pArgs.getTxOp() != null) {
			return pArgs.getTxOp();
		}
		return null;
	}
	*/
}
