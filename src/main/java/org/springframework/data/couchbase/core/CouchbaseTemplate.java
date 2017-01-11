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
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.TranscodingException;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.util.features.CouchbaseFeature;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.AsyncViewRow;
import com.couchbase.client.java.view.SpatialViewQuery;
import com.couchbase.client.java.view.SpatialViewResult;
import com.couchbase.client.java.view.SpatialViewRow;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

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
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;

import static org.springframework.data.couchbase.core.support.TemplateUtils.SELECT_ID;
import static org.springframework.data.couchbase.core.support.TemplateUtils.SELECT_CAS;

/**
 * @author Michael Nitschinger
 * @author Oliver Gierke
 * @author Simon Baslé
 * @author Young-Gu Chae
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
  private final ClusterInfo clusterInfo;


  private ApplicationEventPublisher eventPublisher;
  private WriteResultChecking writeResultChecking = DEFAULT_WRITE_RESULT_CHECKING;
  private PersistenceExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();

  protected final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

  //default value is in case the template isn't constructed through configuration mechanisms that use the setter.
  private Consistency configuredConsistency = Consistency.DEFAULT_CONSISTENCY;

  public CouchbaseTemplate(final ClusterInfo clusterInfo, final Bucket client) {
    this(clusterInfo, client, null, null);
  }

  public CouchbaseTemplate(final ClusterInfo clusterInfo, final Bucket client, final TranslationService translationService) {
    this(clusterInfo, client, null, translationService);
  }

  public CouchbaseTemplate(final ClusterInfo clusterInfo, final Bucket client,
                           final CouchbaseConverter converter,
                           final TranslationService translationService) {
    this.clusterInfo = clusterInfo;
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
    //TODO at some point the necessity of CouchbaseStorable should be re-evaluated
    return translationService.decode(source.content(), target);
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
    doPersist(objectToSave, persistTo, replicateTo, PersistType.SAVE);
  }

  @Override
  public void save(Collection<?> batchToSave) {
    save(batchToSave, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void save(Collection<?> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
    for (Object o : batchToSave) {
      doPersist(o, persistTo, replicateTo, PersistType.SAVE);
    }
  }

  @Override
  public void insert(Object objectToInsert) {
    insert(objectToInsert, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void insert(Object objectToInsert, PersistTo persistTo, ReplicateTo replicateTo) {
    doPersist(objectToInsert, persistTo, replicateTo, PersistType.INSERT);
  }

  @Override
  public void insert(Collection<?> batchToInsert) {
    insert(batchToInsert, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void insert(Collection<?> batchToInsert, PersistTo persistTo, ReplicateTo replicateTo) {
    for (Object o : batchToInsert) {
      doPersist(o, persistTo, replicateTo, PersistType.INSERT);
    }
  }

  @Override
  public void update(Object objectToUpdate) {
    update(objectToUpdate, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void update(Object objectToUpdate, PersistTo persistTo, ReplicateTo replicateTo) {
    doPersist(objectToUpdate, persistTo, replicateTo, PersistType.UPDATE);
  }

  @Override
  public void update(Collection<?> batchToUpdate) {
    update(batchToUpdate, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void update(Collection<?> batchToUpdate, PersistTo persistTo, ReplicateTo replicateTo) {
    for (Object o : batchToUpdate) {
      doPersist(o, persistTo, replicateTo, PersistType.UPDATE);
    }
  }

  @Override
  public <T> T findById(final String id, Class<T> entityClass) {
    final CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
    RawJsonDocument result = execute(new BucketCallback<RawJsonDocument>() {
      @Override
      public RawJsonDocument doInBucket() {
        if (entity.isTouchOnRead()) {
          return client.getAndTouch(id, entity.getExpiry(), RawJsonDocument.class);
        } else {
          return client.get(id, RawJsonDocument.class);
        }
      }
    });

    return mapToEntity(id, result, entityClass);
  }

  @Override
  public <T> List<T> findByView(ViewQuery query, final Class<T> entityClass) {
    //we'll always need to get documents, as a RawJsonDocument, so we should force that target class
    //so that the caller doesn't set a bad target class unintentionally, pre-loading with a bad type.
    if (!query.isIncludeDocs() || !query.includeDocsTarget().equals(RawJsonDocument.class)) {
      if (query.isOrderRetained()) {
        query.includeDocsOrdered(RawJsonDocument.class);
      } else {
        query.includeDocs(RawJsonDocument.class);
      }
    }
    //we'll always map the document to the entity, hence reduce never makes sense.
    query.reduce(false);

    return executeAsync(client.async().query(query))
        .flatMap(new Func1<AsyncViewResult, Observable<AsyncViewRow>>() {
          @Override
          public Observable<AsyncViewRow> call(AsyncViewResult asyncViewResult) {
            return asyncViewResult
                .error()
                .flatMap(new Func1<JsonObject, Observable<AsyncViewRow>>() {
                  @Override
                  public Observable<AsyncViewRow> call(JsonObject error) {
                    return Observable.error(new CouchbaseQueryExecutionException("Unable to execute view query due to the following view error: " + error.toString()));
                  }})
                .switchIfEmpty(asyncViewResult.rows());
          }
        })
        .flatMap(new Func1<AsyncViewRow, Observable<T>>() {
          @Override
          public Observable<T> call(AsyncViewRow row) {
            final String id = row.id();
            return row
                .document(RawJsonDocument.class)
                .map(new Func1<RawJsonDocument, T>() {
                  @Override
                  public T call(RawJsonDocument rawJsonDocument) {
                    //cope with potential weak consistency and deletions
                    T entity = mapToEntity(id, rawJsonDocument, entityClass);
                    return entity;
                  }
                });
          }})
        .filter(new Func1<T, Boolean>() {
          @Override
          public Boolean call(T t) {
            return t != null;
          }
        })
        .onErrorResumeNext(new Func1<Throwable, Observable<T>>() {
          @Override
          public Observable<T> call(Throwable throwable) {
            if (throwable instanceof TranscodingException) {
              return Observable.error(new CouchbaseQueryExecutionException("Unable to execute view query", throwable));
            } else {
              return Observable.error(throwable);
            }
          }
        })
        .toList()
        .toBlocking()
        .single();
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
  public <T> List<T> findBySpatialView(SpatialViewQuery query, Class<T> entityClass) {
    //we'll always need to get documents, as a RawJsonDocument, so we should force includeDocs(false)
    //so that the caller doesn't set a bad target class unintentionally, pre-loading with a bad type.
    query.includeDocs(false);

    try {
      final SpatialViewResult response = querySpatialView(query);
      if (response.error() != null) {
        throw new CouchbaseQueryExecutionException("Unable to execute spatial view query due to the following view error: " +
            response.error().toString());
      }

      List<SpatialViewRow> allRows = response.allRows();

      final List<T> result = new ArrayList<T>(allRows.size());
      for (final SpatialViewRow row : allRows) {
        //cope with potential weak consistency and deletions
        T entity = mapToEntity(row.id(), row.document(RawJsonDocument.class), entityClass);
        if (entity != null) {
          result.add(entity);
        }
      }

      return result;
    }
    catch (TranscodingException e) {
      throw new CouchbaseQueryExecutionException("Unable to execute view query", e);
    }
  }

  @Override
  public SpatialViewResult querySpatialView(final SpatialViewQuery query) {
    return execute(new BucketCallback<SpatialViewResult>() {
      @Override
      public SpatialViewResult doInBucket() throws TimeoutException, ExecutionException, InterruptedException {
        return client.query(query);
      }
    });
  }

  @Override
  public <T> List<T> findByN1QL(N1qlQuery n1ql, Class<T> entityClass) {
    checkN1ql();
    try {
      N1qlQueryResult queryResult = queryN1QL(n1ql);

      if (queryResult.finalSuccess()) {
        List<N1qlQueryRow> allRows = queryResult.allRows();
        List<T> result = new ArrayList<T>(allRows.size());
        for (N1qlQueryRow row : allRows) {
          JsonObject json = row.value();
          String id = json.getString(SELECT_ID);
          Long cas = json.getLong(SELECT_CAS);
          if (id == null || cas == null) {
            throw new CouchbaseQueryExecutionException("Unable to retrieve enough metadata for N1QL to entity mapping, " +
                "have you selected " + SELECT_ID + " and " + SELECT_CAS + "?");
          }
          json = json.removeKey(SELECT_ID).removeKey(SELECT_CAS);
          RawJsonDocument entityDoc = RawJsonDocument.create(id, json.toString(), cas);
          T decoded = mapToEntity(id, entityDoc, entityClass);
          result.add(decoded);
        }
        return result;
      }
      else {
        StringBuilder message = new StringBuilder("Unable to execute query due to the following n1ql errors: ");
        for (JsonObject error : queryResult.errors()) {
          message.append('\n').append(error);
        }
        throw new CouchbaseQueryExecutionException(message.toString());
      }
    }
    catch (TranscodingException e) {
      throw new CouchbaseQueryExecutionException("Unable to execute query", e);
    }
  }

  @Override
  public <T> List<T> findByN1QLProjection(N1qlQuery n1ql, Class<T> entityClass) {
    checkN1ql();
    try {
      N1qlQueryResult queryResult = queryN1QL(n1ql);

      if (queryResult.finalSuccess()) {
        List<N1qlQueryRow> allRows = queryResult.allRows();
        List<T> result = new ArrayList<T>(allRows.size());
        for (N1qlQueryRow row : allRows) {
          JsonObject json = row.value();
          T decoded = translationService.decodeFragment(json.toString(), entityClass);
          result.add(decoded);
        }
        return result;
      }
      else {
        StringBuilder message = new StringBuilder("Unable to execute query due to the following n1ql errors: ");
        for (JsonObject error : queryResult.errors()) {
          message.append('\n').append(error);
        }
        throw new CouchbaseQueryExecutionException(message.toString());
      }
    }
    catch (TranscodingException e) {
      throw new CouchbaseQueryExecutionException("Unable to execute query", e);
    }
  }

  @Override
  public N1qlQueryResult queryN1QL(final N1qlQuery query) {
    checkN1ql();
    return execute(new BucketCallback<N1qlQueryResult>() {
      @Override
      public N1qlQueryResult doInBucket() throws TimeoutException, ExecutionException, InterruptedException {
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

  public <T> Observable<T> executeAsync(Observable<T> asyncAction) {
    return asyncAction
        .onErrorResumeNext(new Func1<Throwable, Observable<T>>() {
          @Override
          public Observable<T> call(Throwable e) {
            if (e instanceof RuntimeException) {
              return Observable.error(exceptionTranslator.translateExceptionIfPossible((RuntimeException) e));
            } else if (e instanceof TimeoutException) {
              return Observable.error(new QueryTimeoutException(e.getMessage(), e));
            } else if (e instanceof InterruptedException) {
              return Observable.error(new OperationInterruptedException(e.getMessage(), e));
            } else if (e instanceof ExecutionException) {
              return Observable.error(new OperationInterruptedException(e.getMessage(), e));
            } else {
              return Observable.error(e);
            }
          }
        });
  }

  private void doPersist(Object objectToPersist, final PersistTo persistTo, final ReplicateTo replicateTo,
                         final PersistType persistType) {
    ensureNotIterable(objectToPersist);

    final ConvertingPropertyAccessor accessor = getPropertyAccessor(objectToPersist);
    final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(objectToPersist.getClass());
    final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();
    final Long version = versionProperty != null ? accessor.getProperty(versionProperty, Long.class) : null;

    maybeEmitEvent(new BeforeConvertEvent<Object>(objectToPersist));
    final CouchbaseDocument converted = new CouchbaseDocument();
    converter.write(objectToPersist, converted);

    maybeEmitEvent(new BeforeSaveEvent<Object>(objectToPersist, converted));
    execute(new BucketCallback<Boolean>() {
      @Override
      public Boolean doInBucket() throws InterruptedException, ExecutionException {
        Document<String> doc = encodeAndWrap(converted, version);
        Document<String> storedDoc;
        //We will check version only if required
        boolean versionPresent = versionProperty != null;
        //If version is not set - assumption that document is new, otherwise updating
        boolean existingDocument = version != null && version > 0L;
        try {
          switch (persistType) {
            case SAVE:
              if (!versionPresent) {
                //No version field - no cas
                storedDoc = client.upsert(doc, persistTo, replicateTo);
              } else if (existingDocument) {
                //Updating existing document with cas
                storedDoc = client.replace(doc, persistTo, replicateTo);
              } else {
                //Creating new document
                storedDoc = client.insert(doc, persistTo, replicateTo);
              }
              break;
            case UPDATE:
              storedDoc = client.replace(doc, persistTo, replicateTo);
              break;
            case INSERT:
            default:
              storedDoc = client.insert(doc, persistTo, replicateTo);
              break;
          }

          if (persistentEntity.hasVersionProperty() && storedDoc != null && storedDoc.cas() != 0) {
            //inject new cas into the bean
            accessor.setProperty(versionProperty, storedDoc.cas());
            return true;
          }
          return false;
        } catch (DocumentAlreadyExistsException e) {
          throw new OptimisticLockingFailureException(persistType.getSpringDataOperationName() +
                  " document with version value failed: " + version, e);
        } catch (CASMismatchException e) {
          throw new OptimisticLockingFailureException(persistType.getSpringDataOperationName() +
              " document with version value failed: " + version, e);
        } catch (Exception e) {
          handleWriteResultError(persistType.getSpringDataOperationName() + " document failed: " + e.getMessage(), e);
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
          try {
            RawJsonDocument deletedDoc = client.remove((String) objectToRemove, persistTo, replicateTo,
                RawJsonDocument.class);
            return deletedDoc != null;
          } catch (Exception e) {
            handleWriteResultError("Delete document failed: " + e.getMessage(), e);
            return false; //this could be skipped if WriteResultChecking.EXCEPTION
          }
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
        try {
          RawJsonDocument deletedDoc = client.remove(converted.getId(), persistTo, replicateTo
              , RawJsonDocument.class);
          return deletedDoc != null;
        } catch (Exception e) {
          handleWriteResultError("Delete document failed: " + e.getMessage(), e);
          return false; //this could be skipped if WriteResultChecking.EXCEPTION
        }
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

    final ConvertingPropertyAccessor accessor = getPropertyAccessor(readEntity);
    CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(readEntity.getClass());
    if (persistentEntity.hasVersionProperty()) {
      accessor.setProperty(persistentEntity.getVersionProperty(), data.cas());
    }

    return (T) readEntity;
  }

  private final ConvertingPropertyAccessor getPropertyAccessor(Object source) {
    CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());
    PersistentPropertyAccessor accessor = entity.getPropertyAccessor(source);

    return new ConvertingPropertyAccessor(accessor, converter.getConversionService());
  }

  private void checkN1ql() {
    if (!getCouchbaseClusterInfo().checkAvailable(CouchbaseFeature.N1QL)) {
      throw new UnsupportedCouchbaseFeatureException("Detected usage of N1QL in template, which is unsupported on this cluster",
          CouchbaseFeature.N1QL);
    }
  }

  @Override
  public Bucket getCouchbaseBucket() {
    return this.client;
  }

  @Override
  public ClusterInfo getCouchbaseClusterInfo() {
    return this.clusterInfo;
  }

  @Override
  public CouchbaseConverter getConverter() {
    return this.converter;
  }

  @Override
  public Consistency getDefaultConsistency() {
    return configuredConsistency;
  }

  public void setDefaultConsistency(Consistency consistency) {
    this.configuredConsistency = consistency;
  }

  private enum PersistType {
    SAVE("Save", "Upsert"),
    INSERT("Insert", "Insert"),
    UPDATE("Update", "Replace");

    private final String sdkOperationName;
    private final String springDataOperationName;

    PersistType(String sdkOperationName, String springDataOperationName) {
      this.sdkOperationName = sdkOperationName;
      this.springDataOperationName = springDataOperationName;
    }

    public String getSdkOperationName() {
      return sdkOperationName;
    }

    public String getSpringDataOperationName() {
      return springDataOperationName;
    }
  }
}
