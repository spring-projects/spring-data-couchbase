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
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.index.CouchbasePersistentEntityIndexCreator;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;

import com.couchbase.client.java.Collection;

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
	private @Nullable CouchbasePersistentEntityIndexCreator indexCreator;

	public CouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter) {
		this(clientFactory, converter, new JacksonTranslationService());
	}

	public CouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter,
			final TranslationService translationService) {
		this.clientFactory = clientFactory;
		this.converter = converter;
		this.templateSupport = new CouchbaseTemplateSupport(converter, translationService);
		this.reactiveCouchbaseTemplate = new ReactiveCouchbaseTemplate(clientFactory, converter, translationService);

		this.mappingContext = this.converter.getMappingContext();
		if (mappingContext instanceof CouchbaseMappingContext) {
			CouchbaseMappingContext cmc = (CouchbaseMappingContext) mappingContext;
			if (cmc.isAutoIndexCreation()) {
				indexCreator = new CouchbasePersistentEntityIndexCreator(cmc, this);
			}
		}
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
	public ExecutableRemoveById removeById() {
		return new ExecutableRemoveByIdOperationSupport(this).removeById();
	}

	@Override
	public ExecutableExistsById existsById() {
		return new ExecutableExistsByIdOperationSupport(this).existsById();
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

	/**
	 * {@inheritDoc}
	 */
	public void setThreadLocalArgs(PseudoArgs pseudoArgs) {
		reactiveCouchbaseTemplate.setThreadLocalArgs(pseudoArgs);
	}

}
