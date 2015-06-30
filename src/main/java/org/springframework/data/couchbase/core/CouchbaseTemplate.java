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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.TranscodingException;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryRow;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.CouchbaseStorable;
import org.springframework.data.couchbase.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.couchbase.core.mapping.event.AfterSaveEvent;
import org.springframework.data.couchbase.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.couchbase.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.couchbase.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.couchbase.core.mapping.event.CouchbaseMappingEvent;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapper;

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
	private PersistenceExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();

	protected final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

	public CouchbaseTemplate(final Bucket client) {
		this(client, null, null);
	}

	public CouchbaseTemplate(final Bucket client, final TranslationService translationService) {
		this(client, null, translationService);
	}

	public CouchbaseTemplate(final Bucket client, final CouchbaseConverter converter,
							 final TranslationService translationService) {
		this.client = client;
		this.converter = converter == null ? getDefaultConverter() : converter;
		this.translationService = translationService == null ? getDefaultTranslationService() : translationService;
		this.mappingContext = this.converter.getMappingContext();
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

	/**
	 * Encode a {@link CouchbaseDocument} into a storable representation (JSON) then prepare
	 * it for storage as a {@link Document}.
	 */
	private Document<String> encodeAndWrap(final CouchbaseDocument source, Long version) {
		String encodedContent = translationService.encode(source);
		if (version == null) {
			return RawJsonDocument.create(source.getId(), source.getExpiration(), encodedContent);
		}
		else {
			return RawJsonDocument.create(source.getId(), source.getExpiration(), encodedContent, version);
		}
	}


	/**
	 * Decode a {@link Document Document&lt;String&gt;} containing a JSON string
	 * into a {@link CouchbaseStorable}
	 */
	private CouchbaseStorable decodeAndUnwrap(final Document<String> source, final CouchbaseStorable target) {
		return translationService.decode(source.content(), target); //TODO rework and check
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
	private void handleWriteResultError(String message, Exception cause) {
		if (writeResultChecking == WriteResultChecking.NONE) {
			return;
		}

		if (writeResultChecking == WriteResultChecking.EXCEPTION) {
			throw new CouchbaseDataIntegrityViolationException(message, cause);
		}
		else {
			LOGGER.error(message, cause);
		}
	}

	public void setWriteResultChecking(WriteResultChecking writeResultChecking) {
		this.writeResultChecking = writeResultChecking == null ? DEFAULT_WRITE_RESULT_CHECKING : writeResultChecking;
	}

	@Override
	public void setApplicationEventPublisher(final ApplicationEventPublisher eventPublisher) {
		this.eventPublisher = eventPublisher;
	}

	/**
	 * Helper method to publish an event if the event publisher is set.
	 *
	 * @param event the event to emit.
	 * @param <T> the enclosed type.
	 */
	protected <T> void maybeEmitEvent(final CouchbaseMappingEvent<T> event) {
		if (eventPublisher != null) {
			eventPublisher.publishEvent(event);
		}
	}

	@Override
	public void save(Object objectToSave) {
		save(objectToSave, PersistTo.NONE, ReplicateTo.NONE);
	}

	@Override
	public void save(Object objectToSave, PersistTo persistTo, ReplicateTo replicateTo) {
		doPersist(objectToSave, persistTo, replicateTo, false, false);
	}

	@Override
	public void save(Collection<?> batchToSave) {
		save(batchToSave, PersistTo.NONE, ReplicateTo.NONE);
	}

	@Override
	public void save(Collection<?> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
		for (Object o : batchToSave) {
			doPersist(o, persistTo, replicateTo, false, false);
		}
	}

	@Override
	public void insert(Object objectToInsert) {
		insert(objectToInsert, PersistTo.NONE, ReplicateTo.NONE);
	}

	@Override
	public void insert(Object objectToInsert, PersistTo persistTo, ReplicateTo replicateTo) {
		doPersist(objectToInsert, persistTo, replicateTo, true, false);
	}

	@Override
	public void insert(Collection<?> batchToInsert) {
		insert(batchToInsert, PersistTo.NONE, ReplicateTo.NONE);
	}

	@Override
	public void insert(Collection<?> batchToInsert, PersistTo persistTo, ReplicateTo replicateTo) {
		for (Object o : batchToInsert) {
			doPersist(o, persistTo, replicateTo, true, false);
		}
	}

	@Override
	public void update(Object objectToUpdate) {
		update(objectToUpdate, PersistTo.NONE, ReplicateTo.NONE);
	}

	@Override
	public void update(Object objectToUpdate, PersistTo persistTo, ReplicateTo replicateTo) {
		doPersist(objectToUpdate, persistTo, replicateTo, false, true);
	}

	@Override
	public void update(Collection<?> batchToUpdate) {
		update(batchToUpdate, PersistTo.NONE, ReplicateTo.NONE);
	}

	@Override
	public void update(Collection<?> batchToUpdate, PersistTo persistTo, ReplicateTo replicateTo) {
		for (Object o : batchToUpdate) {
			doPersist(o, persistTo, replicateTo, false, true);
		}
	}

	@Override
	public <T> T findById(final String id, Class<T> entityClass) {
		RawJsonDocument result = execute(new BucketCallback<RawJsonDocument>() {
			@Override
			public RawJsonDocument doInBucket() {
				return client.get(id, RawJsonDocument.class);
			}
		});

		return mapToEntity(id, result, entityClass);
	}

	@Override
	public <T> List<T> findByView(ViewQuery query, Class<T> entityClass) {
		query.includeDocs(false);
		query.reduce(false);

		try {
			final ViewResult response = queryView(query);
			if (response.error() != null) {
				throw new CouchbaseQueryExecutionException("Unable to execute view query due to the following view error: " +
						response.error().toString());
			}

			List<ViewRow> allRows = response.allRows();

			final List<T> result = new ArrayList<T>(allRows.size());
			for (final ViewRow row : allRows) {
				result.add(mapToEntity(row.id(), row.document(RawJsonDocument.class), entityClass));
			}

			return result;
		} catch (TranscodingException e) {
			throw new CouchbaseQueryExecutionException("Unable to execute view query", e);
		}
	}

	@Override
	public ViewResult queryView(final ViewQuery query) {
		return execute(new BucketCallback<ViewResult>() {
			@Override
			public ViewResult doInBucket() {
				return client.query(query);
			}
		});
	}

	@Override
	public <T> List<T> findByN1QL(Query n1ql, Class<T> entityClass) {
		try {
			QueryResult queryResult = queryN1QL(n1ql);

			if (queryResult.finalSuccess()) {
				List<QueryRow> allRows = queryResult.allRows();
				List<T> result = new ArrayList<T>(allRows.size());
				for (QueryRow row : allRows) {
					String json = row.value().toString();
					T decoded = translationService.decodeFragment(json, entityClass);
					result.add(decoded);
				}
				return result;
			} else {
				StringBuilder message = new StringBuilder("Unable to execute query due to the following n1ql errors: ");
				for (JsonObject error : queryResult.errors()) {
					message.append('\n').append(error);
				}
				throw new CouchbaseQueryExecutionException(message.toString());
			}
		} catch (TranscodingException e) {
			throw new CouchbaseQueryExecutionException("Unable to execute query", e);
		}
	}

	@Override
	public QueryResult queryN1QL(final Query query) {
		return execute(new BucketCallback<QueryResult>() {
			@Override
			public QueryResult doInBucket() throws TimeoutException, ExecutionException, InterruptedException {
				return client.query(query);
			}
		});
	}

	@Override
	public boolean exists(final String id) {
		return execute(new BucketCallback<Boolean>() {
			@Override
			public Boolean doInBucket() throws TimeoutException, ExecutionException, InterruptedException {
				return client.exists(id);
			}
		});
	}

	@Override
	public void remove(Object objectToRemove) {
		remove(objectToRemove, PersistTo.NONE, ReplicateTo.NONE);
	}

	@Override
	public void remove(Object objectToRemove, PersistTo persistTo, ReplicateTo replicateTo) {
		doRemove(objectToRemove, persistTo, replicateTo);
	}

	@Override
	public void remove(Collection<?> batchToRemove) {
		remove(batchToRemove, PersistTo.NONE, ReplicateTo.NONE);
	}

	@Override
	public void remove(Collection<?> batchToRemove, PersistTo persistTo, ReplicateTo replicateTo) {
		for (Object o : batchToRemove) {
			doRemove(o, persistTo, replicateTo);
		}
	}

	@Override
	public <T> T execute(BucketCallback<T> action) {
		try {
			return action.doInBucket();
		}
		catch (RuntimeException e) {
			throw exceptionTranslator.translateExceptionIfPossible(e);
		}
		catch (TimeoutException e) {
			throw new QueryTimeoutException(e.getMessage(), e);
		}
		catch (InterruptedException e) {
			throw new OperationInterruptedException(e.getMessage(), e);
		}
		catch (ExecutionException e) {
			throw new OperationInterruptedException(e.getMessage(), e);
		}
	}

	private void doPersist(Object objectToPersist, final PersistTo persistTo, final ReplicateTo replicateTo,
						   final boolean failOnExist, final boolean failOnMissing) {
		ensureNotIterable(objectToPersist);

		final String operationDesc = failOnExist ? "Insert" : failOnMissing ? "Update" : "Upsert";

		final BeanWrapper<Object> beanWrapper = BeanWrapper.create(objectToPersist, converter.getConversionService());
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(objectToPersist.getClass());
		final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();
		final Long version = versionProperty != null ? beanWrapper.getProperty(versionProperty, Long.class) : null;

		maybeEmitEvent(new BeforeConvertEvent<Object>(objectToPersist));
		final CouchbaseDocument converted = new CouchbaseDocument();
		converter.write(objectToPersist, converted);

		maybeEmitEvent(new BeforeSaveEvent<Object>(objectToPersist, converted));
		execute(new BucketCallback<Boolean>() {
			@Override
			public Boolean doInBucket() throws InterruptedException, ExecutionException {
				Document<String> doc = encodeAndWrap(converted, version);
				Document<String> storedDoc;
				try {
					if (!failOnExist && !failOnMissing) {
						storedDoc = client.upsert(doc, persistTo, replicateTo);
					}
					else if (failOnMissing) {
						storedDoc = client.replace(doc, persistTo, replicateTo);
					}
					else {
						storedDoc = client.insert(doc, persistTo, replicateTo);
					}

					if (persistentEntity.hasVersionProperty() && storedDoc != null && storedDoc.cas() != 0) {
						//inject new cas into the bean
						beanWrapper.setProperty(versionProperty, storedDoc.cas());
						return true;
					}
					return false;
				}
				catch (CASMismatchException e) {
					throw new OptimisticLockingFailureException(operationDesc +
							" document with version value failed: " + version);
				}
				catch (Exception e) {
					handleWriteResultError(operationDesc + " document failed: " + e.getMessage(), e);
					return false; //this could be skipped if WriteResultChecking.EXCEPTION
				}
			}
		});
		maybeEmitEvent(new AfterSaveEvent<Object>(objectToPersist, converted));
	}

	private void doRemove(final Object objectToRemove, final PersistTo persistTo, final ReplicateTo replicateTo) {
		ensureNotIterable(objectToRemove);

		maybeEmitEvent(new BeforeDeleteEvent<Object>(objectToRemove));
		if (objectToRemove instanceof String) {
			execute(new BucketCallback<Boolean>() {
				@Override
				public Boolean doInBucket() throws InterruptedException, ExecutionException {
					RawJsonDocument deletedDoc = client.remove((String) objectToRemove, persistTo, replicateTo,
							RawJsonDocument.class);
					return deletedDoc != null;
				}
			});
			maybeEmitEvent(new AfterDeleteEvent<Object>(objectToRemove));
			return;
		}

		final CouchbaseDocument converted = new CouchbaseDocument();
		converter.write(objectToRemove, converted);

		execute(new BucketCallback<Boolean>() {
			@Override
			public Boolean doInBucket() {
				RawJsonDocument deletedDoc = client.remove(converted.getId(), persistTo, replicateTo
						, RawJsonDocument.class);
				return deletedDoc != null;
			}
		});
		maybeEmitEvent(new AfterDeleteEvent<Object>(objectToRemove));
	}

	private <T> T mapToEntity(String id, Document<String> data, Class<T> entityClass) {
		if (data == null) {
			return null;
		}

		final CouchbaseDocument converted = new CouchbaseDocument(id);
		Object readEntity = converter.read(entityClass, (CouchbaseDocument) decodeAndUnwrap(data, converted));

		final BeanWrapper<Object> beanWrapper = BeanWrapper.create(readEntity, converter.getConversionService());
		CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(readEntity.getClass());
		if (persistentEntity.hasVersionProperty()) {
			beanWrapper.setProperty(persistentEntity.getVersionProperty(), data.cas());
		}

		return (T) readEntity;
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
