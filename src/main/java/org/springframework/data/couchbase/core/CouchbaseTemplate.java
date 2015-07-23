/*
 * Copyright 2013-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.OperationFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
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
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * @author Michael Nitschinger
 * @author Oliver Gierke
 */
public class CouchbaseTemplate implements CouchbaseOperations, ApplicationEventPublisherAware {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTemplate.class);
  private static final Collection<String> ITERABLE_CLASSES;
  private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;

  private final CouchbaseClient client;
  protected final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
  private final CouchbaseExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();
  private final TranslationService translationService;
  private ApplicationEventPublisher eventPublisher;

  private CouchbaseConverter couchbaseConverter;
  private WriteResultChecking writeResultChecking = DEFAULT_WRITE_RESULT_CHECKING;

  static {
    final Set<String> iterableClasses = new HashSet<String>();
    iterableClasses.add(List.class.getName());
    iterableClasses.add(Collection.class.getName());
    iterableClasses.add(Iterator.class.getName());
    ITERABLE_CLASSES = Collections.unmodifiableCollection(iterableClasses);
  }

  public void setWriteResultChecking(final WriteResultChecking resultChecking) {
    writeResultChecking = resultChecking == null ? DEFAULT_WRITE_RESULT_CHECKING : resultChecking;
  }

  public CouchbaseTemplate(final CouchbaseClient client) {
    this(client, null, null);
  }

  public CouchbaseTemplate(final CouchbaseClient client, final TranslationService translationService) {
    this(client, null, translationService);
  }

  public CouchbaseTemplate(final CouchbaseClient client, final CouchbaseConverter couchbaseConverter, final TranslationService translationService) {
    this.client = client;
    this.couchbaseConverter = couchbaseConverter == null ? getDefaultConverter() : couchbaseConverter;
    this.translationService = translationService == null ? getDefaultTranslationService() : translationService;
    mappingContext = this.couchbaseConverter.getMappingContext();
  }

  @Override
  public void setApplicationEventPublisher(final ApplicationEventPublisher eventPublisher) {
    this.eventPublisher = eventPublisher;
  }

  private static CouchbaseConverter getDefaultConverter() {
    final MappingCouchbaseConverter converter = new MappingCouchbaseConverter(new CouchbaseMappingContext());
    converter.afterPropertiesSet();
    return converter;
  }

  private static TranslationService getDefaultTranslationService() {
    final JacksonTranslationService jacksonTranslationService = new JacksonTranslationService();
    jacksonTranslationService.afterPropertiesSet();
    return jacksonTranslationService;
  }

  private Object translateEncode(final CouchbaseStorable source) {
    return translationService.encode(source);
  }


  private CouchbaseStorable translateDecode(final String source, final CouchbaseStorable target) {
    return translationService.decode(source, target);
  }

  @Override
  public final void insert(final Object objectToInsert) {
    insert(objectToInsert, PersistTo.ZERO, ReplicateTo.ZERO);
  }

  @Override
  public final void insert(final Collection<?> batchToInsert) {
    for (final Object toInsert : batchToInsert) {
      insert(toInsert);
    }
  }

  @Override
  public void save(final Object objectToSave) {
    save(objectToSave, PersistTo.ZERO, ReplicateTo.ZERO);
  }

  @Override
  public void save(final Collection<?> batchToSave) {
    for (final Object toSave : batchToSave) {
      save(toSave);
    }
  }

  @Override
  public void update(final Object objectToUpdate) {
    update(objectToUpdate, PersistTo.ZERO, ReplicateTo.ZERO);
  }

  @Override
  public void update(final Collection<?> batchToUpdate) {
    for (final Object toUpdate : batchToUpdate) {
      update(toUpdate);
    }
  }

  @Override
  public final <T> T findById(final String id, final Class<T> entityClass) {
    CASValue result = execute(new BucketCallback<CASValue>() {
      @Override
      public CASValue doInBucket() {
        return client.gets(id);
      }
    });

    if (result == null) {
      return null;
    }

    final CouchbaseDocument converted = new CouchbaseDocument(id);
    Object readEntity = couchbaseConverter.read(entityClass, (CouchbaseDocument) translateDecode(
      (String) result.getValue(), converted));

    CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(readEntity.getClass());
		final PersistentPropertyAccessor accessor = getPropertyAccessor(readEntity);
		
    if (persistentEntity.hasVersionProperty()) {
      accessor.setProperty(persistentEntity.getVersionProperty(), result.getCas());
    }

    return (T) readEntity;
  }


  @Override
  public <T> List<T> findByView(final String designName, final String viewName, final Query query, final Class<T> entityClass) {

    if (query.willIncludeDocs()) {
      query.setIncludeDocs(false);
    }
    if (query.willReduce()) {
      query.setReduce(false);
    }

    final ViewResponse response = queryView(designName, viewName, query);

    final List<T> result = new ArrayList<T>(response.size());
    for (final ViewRow row : response) {
      result.add(findById(row.getId(), entityClass));
    }

    return result;
  }

  @Override
  public ViewResponse queryView(final String designName, final String viewName, final Query query) {
    return execute(new BucketCallback<ViewResponse>() {
      @Override
      public ViewResponse doInBucket() {
        final View view = client.getView(designName, viewName);
        return client.query(view, query);
      }
    });
  }

  @Override
  public void remove(final Object objectToRemove) {
    remove(objectToRemove, PersistTo.ZERO, ReplicateTo.ZERO);
  }

  @Override
  public void remove(final Collection<?> batchToRemove) {
    for (final Object toRemove : batchToRemove) {
      remove(toRemove);
    }
  }

  @Override
  public <T> T execute(final BucketCallback<T> action) {
    try {
      return action.doInBucket();
    } catch (RuntimeException e) {
      throw exceptionTranslator.translateExceptionIfPossible(e);
    } catch (TimeoutException e) {
      throw new QueryTimeoutException(e.getMessage(), e);
    } catch (InterruptedException e) {
      throw new OperationInterruptedException(e.getMessage(), e);
    } catch (ExecutionException e) {
      throw new OperationInterruptedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean exists(final String id) {
    final String result = execute(new BucketCallback<String>() {
      @Override
      public String doInBucket() {
        return (String) client.get(id);
      }
    });
    return result != null;
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

  @Override
  public CouchbaseConverter getConverter() {
    return couchbaseConverter;
  }

  @Override
  public void save(Object objectToSave, final PersistTo persistTo, final ReplicateTo replicateTo) {
    ensureNotIterable(objectToSave);

    CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(objectToSave.getClass());
		final ConvertingPropertyAccessor accessor = getPropertyAccessor(objectToSave);
    final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();
    final Long version = versionProperty != null ? accessor.getProperty(versionProperty, Long.class) : null;

    maybeEmitEvent(new BeforeConvertEvent<Object>(objectToSave));
    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToSave, converted);

    maybeEmitEvent(new BeforeSaveEvent<Object>(objectToSave, converted));
    execute(new BucketCallback<Boolean>() {
      @Override
      public Boolean doInBucket() throws InterruptedException, ExecutionException {
        if (version == null) {
          OperationFuture<Boolean> setFuture = client.set(converted.getId(), converted.getExpiration(),
            translateEncode(converted), persistTo, replicateTo);
          boolean future = setFuture.get();
          if (!future) {
            handleWriteResultError("Saving document failed: " + setFuture.getStatus().getMessage());
          }
          return future;
        } else {
          OperationFuture<CASResponse> casFuture = client.asyncCas(converted.getId(), version,
            converted.getExpiration(), translateEncode(converted), persistTo, replicateTo);
          CASResponse cas = casFuture.get();
          if (cas == CASResponse.EXISTS) {
            throw new OptimisticLockingFailureException("Saving document with version value failed: " + cas);
          } else {
            Long casValue = casFuture.getCas();
            long newCas = (casValue == null) ? 0 : casValue.longValue();
            accessor.setProperty(versionProperty, newCas);
            return true;
          }
        }
      }
    });
    maybeEmitEvent(new AfterSaveEvent<Object>(objectToSave, converted));
  }

  @Override
  public void save(Collection<?> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
    for (Object toSave : batchToSave) {
      save(toSave, persistTo, replicateTo);
    }
  }

  @Override
  public void insert(Object objectToInsert, final PersistTo persistTo, final ReplicateTo replicateTo) {
    ensureNotIterable(objectToInsert);

    final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(objectToInsert.getClass());
    final ConvertingPropertyAccessor accessor = getPropertyAccessor(objectToInsert);

    if (persistentEntity != null && persistentEntity.hasVersionProperty()) {
      final Long version = accessor.getProperty(persistentEntity.getVersionProperty(), Long.class);
      if (version == 0) {
        accessor.setProperty(persistentEntity.getVersionProperty(), 0);
      }
    }

    maybeEmitEvent(new BeforeConvertEvent<Object>(objectToInsert));
    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToInsert, converted);

    maybeEmitEvent(new BeforeSaveEvent<Object>(objectToInsert, converted));
    execute(new BucketCallback<Boolean>() {
      @Override
      public Boolean doInBucket() throws InterruptedException, ExecutionException {
        OperationFuture<Boolean> addFuture = client.add(converted.getId(), converted.getExpiration(),
          translateEncode(converted), persistTo, replicateTo);
        boolean result = addFuture.get();
        if(result == false) {
          handleWriteResultError("Inserting document failed: "
            + addFuture.getStatus().getMessage());
        }

        if (result && persistentEntity.hasVersionProperty()) {
          accessor.setProperty(persistentEntity.getVersionProperty(), addFuture.getCas());
        }
        return result;
      }
    });
    maybeEmitEvent(new AfterSaveEvent<Object>(objectToInsert, converted));
  }

  @Override
  public void insert(Collection<?> batchToInsert, PersistTo persistTo, ReplicateTo replicateTo) {
    for (Object toInsert : batchToInsert) {
      insert(toInsert, persistTo,replicateTo);
    }
  }

  @Override
  public void update(Object objectToUpdate, final PersistTo persistTo, final ReplicateTo replicateTo) {
    ensureNotIterable(objectToUpdate);

		CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(objectToUpdate.getClass());
    final ConvertingPropertyAccessor accessor = getPropertyAccessor(objectToUpdate);
    final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();
    final Long version = versionProperty != null ? accessor.getProperty(versionProperty, Long.class) : null;

    maybeEmitEvent(new BeforeConvertEvent<Object>(objectToUpdate));
    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToUpdate, converted);

    maybeEmitEvent(new BeforeSaveEvent<Object>(objectToUpdate, converted));
    execute(new BucketCallback<Boolean>() {
      @Override
      public Boolean doInBucket() throws InterruptedException, ExecutionException {
        if (version == null) {
          return client.replace(converted.getId(), converted.getExpiration(), translateEncode(converted), persistTo,
            replicateTo).get();
        } else {
          OperationFuture<CASResponse> casFuture = client.asyncCas(converted.getId(), version,
            converted.getExpiration(), translateEncode(converted), persistTo, replicateTo);
          CASResponse cas = casFuture.get();

          if (cas == CASResponse.EXISTS) {
            throw new OptimisticLockingFailureException("Updating document with version value failed: " + cas);
          } else {
            long newCas = casFuture.getCas();
            accessor.setProperty(versionProperty, newCas);
            return true;
          }
        }
      }
    });
    maybeEmitEvent(new AfterSaveEvent<Object>(objectToUpdate, converted));
  }

  @Override
  public void update(Collection<?> batchToUpdate, PersistTo persistTo, ReplicateTo replicateTo) {
    for (Object toUpdate : batchToUpdate) {
      update(toUpdate, persistTo, replicateTo);
    }
  }

  @Override
  public void remove(final Object objectToRemove, final PersistTo persistTo, final ReplicateTo replicateTo) {
    ensureNotIterable(objectToRemove);

    maybeEmitEvent(new BeforeDeleteEvent<Object>(objectToRemove));
    if (objectToRemove instanceof String) {
      execute(new BucketCallback<Boolean>() {
        @Override
        public Boolean doInBucket() throws InterruptedException, ExecutionException {
          return client.delete((String) objectToRemove, persistTo, replicateTo).get();
        }
      });
      maybeEmitEvent(new AfterDeleteEvent<Object>(objectToRemove));
      return;
    }

    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToRemove, converted);

    execute(new BucketCallback<OperationFuture<Boolean>>() {
      @Override
      public OperationFuture<Boolean> doInBucket() {
        return client.delete(converted.getId());
      }
    });
    maybeEmitEvent(new AfterDeleteEvent<Object>(objectToRemove));
  }

  @Override
  public void remove(Collection<?> batchToRemove, PersistTo persistTo, ReplicateTo replicateTo) {
    for (Object toRemove : batchToRemove) {
      remove(toRemove, persistTo, replicateTo);
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
  public CouchbaseClient getCouchbaseClient() {
    return client;
  }
  
  private final ConvertingPropertyAccessor getPropertyAccessor(Object source) {
  	
  	CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());
  	PersistentPropertyAccessor accessor = entity.getPropertyAccessor(source);
  	
  	return new ConvertingPropertyAccessor(accessor, couchbaseConverter.getConversionService());
  }
}
