/*
 * Copyright 2012-2019 the original author or authors
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


import java.time.Duration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.error.KeyExistsException;
import com.couchbase.client.core.error.KeyNotFoundException;
import com.couchbase.client.core.error.QueryException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.JsonTranscoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.core.error.CASMismatchException;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.query.QueryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.convert.join.N1qlJoinResolver;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.KeySettings;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.core.query.N1qlJoin;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.util.TypeInformation;
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

import static org.springframework.data.couchbase.core.support.TemplateUtils.SELECT_CAS;
import static org.springframework.data.couchbase.core.support.TemplateUtils.SELECT_ID;

/**
 * @author Michael Nitschinger
 * @author Oliver Gierke
 * @author Simon Baslé
 * @author Young-Gu Chae
 * @author Mark Paluch
 * @author Tayeb Chlyah
 * @author David Kelly
 */
public class CouchbaseTemplate implements CouchbaseOperations, ApplicationEventPublisherAware {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTemplate.class);
  private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;
  private static final java.util.Collection<String> ITERABLE_CLASSES;

  static {
    final Set<String> iterableClasses = new HashSet<String>();
    iterableClasses.add(List.class.getName());
    iterableClasses.add(java.util.Collection.class.getName());
    iterableClasses.add(Iterator.class.getName());
    ITERABLE_CLASSES = java.util.Collections.unmodifiableCollection(iterableClasses);
  }

  private final Collection client;
  private final CouchbaseConverter converter;
  private final TranslationService translationService;
  private final Cluster cluster;
  private KeySettings keySettings;


  private ApplicationEventPublisher eventPublisher;
  private WriteResultChecking writeResultChecking = DEFAULT_WRITE_RESULT_CHECKING;
  private PersistenceExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();

  protected final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

  //default value is in case the template isn't constructed through configuration mechanisms that use the setter.
  private Consistency configuredConsistency = Consistency.DEFAULT_CONSISTENCY;

  public CouchbaseTemplate(final Cluster cluster, final Collection client) {
    this(cluster, client, null, null);
  }

  public CouchbaseTemplate(final Cluster cluster, final Collection client, final TranslationService translationService) {
    this(cluster, client, null, translationService);
  }

  public CouchbaseTemplate(final Cluster cluster,
                           final Collection client,
                           final CouchbaseConverter converter,
                           final TranslationService translationService) {
    this.cluster = cluster;
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

  /**
   * Configures the WriteResultChecking to be used with the template. Setting null will reset
   * the default of DEFAULT_WRITE_RESULT_CHECKING. This can be configured to capture couchbase
   * specific exceptions like Temporary failure, Authentication failure..
   *
   * @param writeResultChecking the setting to use.
   */
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
  public void save(java.util.Collection<?> batchToSave) {
    save(batchToSave, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void save(java.util.Collection<?> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
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
  public void insert(java.util.Collection<?> batchToInsert) {
    insert(batchToInsert, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void insert(java.util.Collection<?> batchToInsert, PersistTo persistTo, ReplicateTo replicateTo) {
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
  public void update(java.util.Collection<?> batchToUpdate) {
    update(batchToUpdate, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void update(java.util.Collection<?> batchToUpdate, PersistTo persistTo, ReplicateTo replicateTo) {
    for (Object o : batchToUpdate) {
      doPersist(o, persistTo, replicateTo, PersistType.UPDATE);
    }
  }

  @Override
  public <T> T findById(final String id, Class<T> entityClass) {
    final CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
    return execute(new CollectionCallback<T>() {
      @Override
      public T doInCollection() {
        GetResult res;
        if (entity.isTouchOnRead()) {
          Duration exp = Duration.ofSeconds((long)entity.getExpiry());
          res = client.getAndTouch(id, exp);
        } else {
          res = client.get(id);
        }
        return mapToEntity(id, res.contentAs(entityClass), res.cas());
      }
    });
  }


  @Override
  public <T> List<T> findByN1QL(N1QLQuery n1ql, Class<T> entityClass) {

    try {
      // TODO: we need to write a custom Serializer.  For now we will convert to json
      //       and grab the cas and id fields from that, then mapToEntity.  This has
      //       efficiency issues (right now mostly space) which we should eliminate
      QueryResult result = queryN1QL(n1ql);

      // mapped entities end up in this list
      List<T> returnVal = new ArrayList<>();

      // for now lets get 2 lists and use the json to get the cas and id, then
      // we can feed the T into mapToEntity for now.
      List<JsonObject> jsonRows = result.rowsAs(JsonObject.class);
      List<T> objRows = result.rowsAs(entityClass);

      Iterator<JsonObject> jsonIt = jsonRows.iterator();
      Iterator<T> objIt = objRows.iterator();
      while (jsonIt.hasNext() && objIt.hasNext()) {
        String id = jsonIt.next().getString(SELECT_ID);
        Long cas = jsonIt.next().getLong(SELECT_CAS);
        T obj = objIt.next();

        // append to end of return value list (maintaining order that way)
        returnVal.add(mapToEntity(id, obj, cas));
      }
      return returnVal;

    } catch (QueryException e) {
      StringBuilder message = new StringBuilder("Unable to execute query due to the following n1ql errors: ");
      for ( ErrorCodeAndMessage error : e.errors()) {
        // nice to have the code in there, so lets just toString it
        message.append('\n').append(error);
      }
      throw new CouchbaseQueryExecutionException(message.toString());
    }
  }

  @Override
  public <T> List<T> findByN1QLProjection(N1QLQuery n1ql, Class<T> entityClass) {
    try {
      return queryN1QL(n1ql).rowsAs(entityClass);
    }
    catch (QueryException e) {
      StringBuilder message = new StringBuilder("Unable to execute query due to the following n1ql errors: ");
      for (ErrorCodeAndMessage error : e.errors()) {
        message.append('\n').append(error);
      }
      throw new CouchbaseQueryExecutionException(message.toString());
    }
  }

  @Override
  public QueryResult queryN1QL(final N1QLQuery query) {
    return execute(new CollectionCallback<QueryResult>() {
      @Override
      public QueryResult doInCollection() {
        return cluster.query(query.getExpression(), query.getOptions());
      }
    });
  }

  @Override
  public boolean exists(final String id) {
    return execute(new CollectionCallback<Boolean>() {
      @Override
      public Boolean doInCollection() throws TimeoutException, ExecutionException, InterruptedException {
        try {
          client.exists(id);
          return true;
        } catch (KeyNotFoundException e) {
          return false;
        }
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
  public void remove(java.util.Collection<?> batchToRemove) {
    remove(batchToRemove, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void remove(java.util.Collection<?> batchToRemove, PersistTo persistTo, ReplicateTo replicateTo) {
    for (Object o : batchToRemove) {
      doRemove(o, persistTo, replicateTo);
    }
  }

  @Override
  public <T> T execute(CollectionCallback<T> action) {
    try {
      return action.doInCollection();
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

    final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(objectToPersist);
    final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(objectToPersist.getClass());
    final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();
    final Long version = versionProperty != null ? accessor.getProperty(versionProperty, Long.class) : null;

    maybeEmitEvent(new BeforeConvertEvent<Object>(objectToPersist));
    final CouchbaseDocument converted = new CouchbaseDocument();
    converter.write(objectToPersist, converted);

    maybeEmitEvent(new BeforeSaveEvent<Object>(objectToPersist, converted));
    execute(new CollectionCallback<Boolean>() {
      @Override
      public Boolean doInCollection() throws InterruptedException, ExecutionException {
        String generatedId = addCommonPrefixAndSuffix(converted.getId());
        converted.setId(generatedId);
        //We will check version only if required
        boolean versionPresent = versionProperty != null;
        //If version is not set - assumption that document is new, otherwise updating
        boolean existingDocument = version != null && version > 0L;

        long cas = 0;
        try {
          switch (persistType) {
            case SAVE:
              if (!versionPresent) {
                //No version field - no cas
                cas = client.upsert(converted.getId(), converted.getPayload(), UpsertOptions.upsertOptions().durability(persistTo, replicateTo)).cas();
              } else if (existingDocument) {
                //Updating existing document with cas
                cas = client.replace(converted.getId(), converted.getPayload(), ReplaceOptions.replaceOptions().durability(persistTo, replicateTo)).cas();
              } else {
                //Creating new document
                cas = client.insert(converted.getId(), converted.getPayload(), InsertOptions.insertOptions().durability(persistTo, replicateTo)).cas();
              }
              break;
            case UPDATE:
              cas = client.replace(converted.getId(), converted.getPayload(), ReplaceOptions.replaceOptions().durability(persistTo, replicateTo)).cas();
              break;
            case INSERT:
            default:
              cas = client.insert(converted.getId(), converted.getPayload(), InsertOptions.insertOptions().durability(persistTo, replicateTo)).cas();
              break;
          }
          CouchbasePersistentProperty idProperty = persistentEntity.getIdProperty();
		  Object entityId = accessor.getProperty(idProperty);
          if (!generatedId.equals(entityId)) {
                accessor.setProperty(idProperty, generatedId);
          }

          if (cas != 0) {
            //inject new cas into the bean
            if (versionProperty != null) {
              accessor.setProperty(versionProperty, cas);
            }
            return true;
          }
          return false;
        } catch (KeyExistsException e) {
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
      execute(new CollectionCallback<Boolean>() {
        @Override
        public Boolean doInCollection() throws InterruptedException, ExecutionException {
          try {
            client.remove((String) objectToRemove , RemoveOptions.removeOptions().durability(persistTo, replicateTo));
            return true;
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

    execute(new CollectionCallback<Boolean>() {
      @Override
      public Boolean doInCollection() {
        try {
          client.remove(addCommonPrefixAndSuffix(converted.getId()), RemoveOptions.removeOptions().durability(persistTo, replicateTo));
          return true;
        } catch (Exception e) {
          handleWriteResultError("Delete document failed: " + e.getMessage(), e);
          return false; //this could be skipped if WriteResultChecking.EXCEPTION
        }
      }
    });
    maybeEmitEvent(new AfterDeleteEvent<Object>(objectToRemove));
  }

  private <T> T mapToEntity(String id, T readEntity, long cas) {

    if (readEntity == null) {
      return null;
    }

    final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(readEntity);
    CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(readEntity.getClass());

    if (persistentEntity.getVersionProperty() != null) {
      accessor.setProperty(persistentEntity.getVersionProperty(), cas);
    }

    persistentEntity.doWithProperties((PropertyHandler<CouchbasePersistentProperty>) prop -> {
      if (prop.isAnnotationPresent(N1qlJoin.class)) {
        N1qlJoin definition = prop.findAnnotation(N1qlJoin.class);
        TypeInformation type =  prop.getTypeInformation().getActualType();
        Class clazz = type.getType();
        N1qlJoinResolver.N1qlJoinResolverParameters parameters = new N1qlJoinResolver.N1qlJoinResolverParameters(definition, id, persistentEntity.getTypeInformation(), type);
        if (N1qlJoinResolver.isLazyJoin(definition)) {
          N1qlJoinResolver.N1qlJoinProxy proxy = new N1qlJoinResolver.N1qlJoinProxy(this, parameters);
          accessor.setProperty(prop, java.lang.reflect.Proxy.newProxyInstance(List.class.getClassLoader(),
                  new Class[]{List.class}, proxy));
        } else {
          accessor.setProperty(prop, N1qlJoinResolver.doResolve(this, parameters, clazz));
        }
      }
    });

    return accessor.getBean();
  }

  private final <T> ConvertingPropertyAccessor<T> getPropertyAccessor(T source) {

    CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(source.getClass());
    PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(source);

    return new ConvertingPropertyAccessor<>(accessor, converter.getConversionService());
  }

/*  I believe this isn't necessary - we wont be supporting servers from before n1ql
  private void checkN1ql() {
    if (!getCouchbaseClusterConfig().clusterCapabilities(CouchbaseFeature.N1QL)) {
      throw new UnsupportedCouchbaseFeatureException("Detected usage of N1QL in template, which is unsupported on this cluster",
          CouchbaseFeature.N1QL);
    }
  } */

  @Override
  public Bucket getCouchbaseBucket() {
    return cluster.bucket(client.bucketName());
  }

  @Override
  public CouchbaseConverter getConverter() {
    return this.converter;
  }

  @Override
  public ClusterConfig getCouchbaseClusterConfig() { return cluster.core().clusterConfig(); }

  @Override
  public Collection getCouchbaseCollection() { return client; }

  @Override
  public Cluster getCouchbaseCluster() { return cluster; }

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

  @Override
  public void keySettings(KeySettings settings) {
    if (this.keySettings != null) {
      throw new UnsupportedOperationException("Key settings is already set, it is no longer mutable");
    }
    this.keySettings = settings;
  }

  @Override
  public KeySettings keySettings() {
    return this.keySettings;
  }

  @Override
  public String getGeneratedId(Object entity) {
    ensureNotIterable(entity);
    CouchbaseDocument converted = new CouchbaseDocument();
    converter.write(entity, converted);
    return addCommonPrefixAndSuffix(converted.getId());
  }

  private String addCommonPrefixAndSuffix(final String id) {
    String convertedKey = id;
    if (this.keySettings == null) {
      return id;
    }
    String prefix = this.keySettings.prefix();
    String delimiter = this.keySettings.delimiter();
    String suffix = this.keySettings.suffix();
    if (prefix != null && !prefix.equals("")) {
      convertedKey = prefix + delimiter + convertedKey;
    }
    if (suffix != null && !suffix.equals("")) {
      convertedKey = convertedKey + delimiter + suffix;
    }
    return convertedKey;
  }
}
