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

import com.couchbase.client.java.ClusterInterface;
import org.springframework.context.ApplicationListener;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.transaction.ClientSession;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseClientUtils;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.transaction.SessionSynchronization;
import org.springframework.data.mapping.context.MappingContextEvent;
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
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.data.couchbase.transaction.CouchbaseStuffHandle;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.query.QueryScanConsistency;

import java.util.function.Consumer;

import static org.springframework.data.couchbase.repository.support.Util.hasNonZeroVersionProperty;

/**
 * template class for Reactive Couchbase operations
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jorge Rodriguez Martin
 * @author Carlos Espinaco
 */
public class ReactiveCouchbaseTemplate implements ReactiveCouchbaseOperations, ApplicationContextAware {

	private final ReactiveCouchbaseClientFactory clientFactory;
	private final CouchbaseConverter converter;
	private final PersistenceExceptionTranslator exceptionTranslator;
	private final ReactiveCouchbaseTemplateSupport templateSupport;
	private ThreadLocal<PseudoArgs<?>> threadLocalArgs = new ThreadLocal<>();
	private QueryScanConsistency scanConsistency;

	//public ReactiveCouchbaseTemplate with(CouchbaseStuffHandle txOp) {
		// TODO: why does txOp go on the clientFactory? can't we just put it on the template??
	//	return new ReactiveCouchbaseTemplate(getCouchbaseClientFactory().with(txOp), getConverter(),
	//			support().getTranslationService(), getConsistency());
	//}

	//public CouchbaseStuffHandle txOperator() {
	//	return clientFactory.getTransactionalOperator();
	//}

	public ReactiveCouchbaseTemplate(final ReactiveCouchbaseClientFactory clientFactory,
			final CouchbaseConverter converter) {
		this(clientFactory, converter, new JacksonTranslationService(), null);
	}

	public ReactiveCouchbaseTemplate(final ReactiveCouchbaseClientFactory clientFactory,
			final CouchbaseConverter converter, final TranslationService translationService,
            final QueryScanConsistency scanConsistency) {
		this.clientFactory = clientFactory;
		this.converter = converter;
		this.exceptionTranslator = clientFactory.getExceptionTranslator();
		this.templateSupport = new ReactiveCouchbaseTemplateSupport(this, converter, translationService);
		this.scanConsistency = scanConsistency;
	}

	// public ReactiveCouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter) {
	// this(clientFactory, converter, new JacksonTranslationService());
	// }

	// public ReactiveCouchbaseTemplate(final ReactiveCouchbaseClientFactory clientFactory, final CouchbaseConverter
	// converter,
	// final TranslationService translationService) {
	// this.clientFactory = clientFactory;
	// this.converter = converter;
	// this.exceptionTranslator = this.clientFactory.getExceptionTranslator();
	// this.templateSupport = new ReactiveCouchbaseTemplateSupport(this, converter, translationService);
	// }

	public <T> Mono<T> save(T entity) {
		if (hasNonZeroVersionProperty(entity, templateSupport.converter)) {
			return replaceById((Class<T>) entity.getClass()).one(entity);
		//} else if (getTransactionalOperator() != null) {
		//	return insertById((Class<T>) entity.getClass()).one(entity);
			 } else {
			return upsertById((Class<T>) entity.getClass()).one(entity);
		}
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
		return clientFactory.getBucket().block().name();
	}

	@Override
	public String getScopeName() {
		return clientFactory.getScope().block().name();
	}

	@Override
	public ReactiveCouchbaseClientFactory getCouchbaseClientFactory() {
		return clientFactory;
	}

	/**
	 * Provides access to a {@link Collection} on the configured {@link CouchbaseClientFactory}.
	 *
	 * @param collectionName the name of the collection, if null is passed in the default collection is assumed.
	 * @return the collection instance.
	 */
	public Collection getCollection(final String collectionName) {
		return clientFactory.getCollection(collectionName).block();
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

	protected Mono<ClusterInterface> doGetDatabase() {
		return ReactiveCouchbaseClientUtils.getDatabase(clientFactory, SessionSynchronization.ON_ACTUAL_TRANSACTION);
	}

	protected Mono<ReactiveCouchbaseTemplate> doGetTemplate() {
		return ReactiveCouchbaseClientUtils.getTemplate(clientFactory, SessionSynchronization.ON_ACTUAL_TRANSACTION,
				this.getConverter());
	}

	/*
	private <T> Flux<T> withSession(ReactiveSessionCallback<T> action, ClientSession session) {
	
		ReactiveSessionBoundCouchbaseTemplate operations = new ReactiveSessionBoundCouchbaseTemplate(session,
				ReactiveCouchbaseTemplate.this);
	
		return Flux.from(action.doInSession(operations)) //
				.contextWrite(ctx -> ReactiveMongoContext.setSession(ctx, Mono.just(session)));
	}
	*/
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#withSession(com.mongodb.session.ClientSession)
	 */
	public ReactiveCouchbaseOperations withSession(ClientSession session) {
		return new ReactiveSessionBoundCouchbaseTemplate(session, ReactiveCouchbaseTemplate.this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#withSession(com.mongodb.ClientSessionOptions)
	 */
	/*
	@Override
	public ReactiveSessionScoped withSession(ClientSessionOptions sessionOptions) {
		return withSession(mongoDatabaseFactory.getSession(sessionOptions));
	}
	
	 */

	/**
	 * {@link CouchbaseTemplate} extension bound to a specific {@link ClientSession} that is applied when interacting with
	 * the server through the driver API. <br />
	 * The prepare steps for {} and {} proxy the target and invoke the desired target method matching the actual arguments
	 * plus a {@link ClientSession}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class ReactiveSessionBoundCouchbaseTemplate extends ReactiveCouchbaseTemplate {

		private final ReactiveCouchbaseTemplate delegate;
		private final ClientSession session;

		/**
		 * @param session must not be {@literal null}.
		 * @param that must not be {@literal null}.
		 */
		ReactiveSessionBoundCouchbaseTemplate(ClientSession session, ReactiveCouchbaseTemplate that) {

			super(that.clientFactory.withSession(session), that.getConverter());

			this.delegate = that;
			this.session = session;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveMongoTemplate#getCollection(java.lang.String)
		 */
		@Override
		public Collection getCollection(String collectionName) {

			// native MongoDB objects that offer methods with ClientSession must not be proxied.
			return delegate.getCollection(collectionName);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveMongoTemplate#getMongoDatabase()
		 */
		@Override
		public ReactiveCouchbaseClientFactory getCouchbaseClientFactory() {

			// native MongoDB objects that offer methods with ClientSession must not be proxied.
			return delegate.getCouchbaseClientFactory();
		}
	}

	class IndexCreatorEventListener implements ApplicationListener<MappingContextEvent<?, ?>> {

		final Consumer<Throwable> subscriptionExceptionHandler;

		public IndexCreatorEventListener(Consumer<Throwable> subscriptionExceptionHandler) {
			this.subscriptionExceptionHandler = subscriptionExceptionHandler;
		}

		@Override
		public void onApplicationEvent(MappingContextEvent<?, ?> event) {

			if (!event.wasEmittedBy(converter.getMappingContext())) {
				return;
			}

			// PersistentEntity<?, ?> entity = event.getPersistentEntity();

			// Double check type as Spring infrastructure does not consider nested generics
			// if (entity instanceof MongoPersistentEntity) {
			// onCheckForIndexes((MongoPersistentEntity<?>) entity, subscriptionExceptionHandler);
			// }
		}
	}

	/**
	 * Get the TransactionalOperator from <br>
	 * 1. The template.clientFactory<br>
	 * 2. The template.threadLocal<br>
	 * 3. otherwise null<br>
	 * This can be overriden in the operation method by<br>
	 * 1. repository.withCollection()
	 */
	/*
	private CouchbaseStuffHandle getTransactionalOperator() {
		if (this.getCouchbaseClientFactory().getTransactionalOperator() != null) {
			return this.getCouchbaseClientFactory().getTransactionalOperator();
		}
		ReactiveCouchbaseTemplate t = this;
		PseudoArgs pArgs = t.getPseudoArgs();
		if (pArgs != null && pArgs.getTxOp() != null) {
			return pArgs.getTxOp();
		}
		return null;
	}
	 */
	/**
	 * Value object chaining together a given source document with its mapped representation and the collection to persist
	 * it to.
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.2
	 */
	/*
	private static class PersistableEntityModel<T> {
	
		private final T source;
		private final @Nullable
		Document target;
		private final String collection;
	
		private PersistableEntityModel(T source, @Nullable Document target, String collection) {
	
			this.source = source;
			this.target = target;
			this.collection = collection;
		}
	
		static <T> PersistableEntityModel<T> of(T source, String collection) {
			return new PersistableEntityModel<>(source, null, collection);
		}
	
		static <T> PersistableEntityModel<T> of(T source, Document target, String collection) {
			return new PersistableEntityModel<>(source, target, collection);
		}
	
		PersistableEntityModel<T> mutate(T source) {
			return new PersistableEntityModel(source, target, collection);
		}
	
		PersistableEntityModel<T> addTargetDocument(Document target) {
			return new PersistableEntityModel(source, target, collection);
		}
	
		T getSource() {
			return source;
		}
	
		@Nullable
		Document getTarget() {
			return target;
		}
	
		String getCollection() {
			return collection;
		}
	
	 */
}
