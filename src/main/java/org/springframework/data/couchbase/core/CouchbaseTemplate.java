/*
 * Copyright 2013-2014 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapper;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

/**
 * @author Michael Nitschinger
 * @author Oliver Gierke
 */
public class CouchbaseTemplate implements CouchbaseOperations, ApplicationEventPublisherAware {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTemplate.class);
  private static final Collection<String> ITERABLE_CLASSES;
  private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;

  private final Bucket client;
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

  public CouchbaseTemplate(final Bucket client) {
    this(client, null, null);
  }

  public CouchbaseTemplate(final Bucket client, final TranslationService translationService) {
    this(client, null, translationService);
  }

  public CouchbaseTemplate(final Bucket client, final CouchbaseConverter couchbaseConverter,
      final TranslationService translationService) {
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

  private StringDocument translateEncode(final CouchbaseDocument source, final long version) {
    final String json = translationService.encode(source);
    return StringDocument.create(source.getId(), source.getExpiration(), json, version);
  }


  private CouchbaseStorable translateDecode(final String source, final CouchbaseStorable target) {
    return translationService.decode(source, target);
  }

  @Override
  public final void insert(final Object objectToInsert) {
    insert(objectToInsert, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public final void insert(final Collection<?> batchToInsert) {
    for (final Object toInsert : batchToInsert) {
      insert(toInsert);
    }
  }

  @Override
  public void save(final Object objectToSave) {
    save(objectToSave, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void save(final Collection<?> batchToSave) {
    for (final Object toSave : batchToSave) {
      save(toSave);
    }
  }

  @Override
  public void update(final Object objectToUpdate) {
    update(objectToUpdate, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void update(final Collection<?> batchToUpdate) {
    for (final Object toUpdate : batchToUpdate) {
      update(toUpdate);
    }
  }

  @Override
  public final <T> T findById(final String id, final Class<T> entityClass) {
    StringDocument result = execute(new BucketCallback<StringDocument>() {
      @Override
      public StringDocument doInBucket() {
        final StringDocument document = StringDocument.create(id);
        return client.get(document);
      }
    });

    if (result == null) {
      return null;
    }

    final CouchbaseDocument converted = new CouchbaseDocument(id);
    Object readEntity =
        couchbaseConverter.read(entityClass,
            (CouchbaseDocument) translateDecode(result.content().toString(), converted));

    final BeanWrapper<Object> beanWrapper =
        BeanWrapper.create(readEntity, couchbaseConverter.getConversionService());
    CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(readEntity.getClass());
    if (persistentEntity.hasVersionProperty()) {
      beanWrapper.setProperty(persistentEntity.getVersionProperty(), result.cas());
    }

    return (T) readEntity;
  }


  @Override
  public <T> List<T> findByView(final ViewQuery query, final Class<T> entityClass) {
    final ViewResult response = queryView(query);

    final List<T> result = new ArrayList<T>();
    for (final ViewRow row : response.allRows()) {
      result.add(findById(row.id(), entityClass));
    }

    return result;
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
  public void remove(final Object objectToRemove) {
    remove(objectToRemove, PersistTo.NONE, ReplicateTo.NONE);
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
    final StringDocument documentId = StringDocument.create(id);
    return client.get(documentId) != null;
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

    final BeanWrapper<Object> beanWrapper = BeanWrapper.create(objectToSave, couchbaseConverter.getConversionService());
    CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(objectToSave.getClass());
    final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();
    final Long version =
        versionProperty != null ? beanWrapper.getProperty(versionProperty, Long.class) : 0L;

    maybeEmitEvent(new BeforeConvertEvent<Object>(objectToSave));
    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToSave, converted);

    maybeEmitEvent(new BeforeSaveEvent<Object>(objectToSave, converted));
    execute(new BucketCallback<Boolean>() {
      @Override
      public Boolean doInBucket() throws InterruptedException, ExecutionException {
        final StringDocument translateEncode = translateEncode(converted, version);
        try {
          final StringDocument document = client.replace(translateEncode, persistTo, replicateTo);
          if (versionProperty != null) {
            final long newCas = document.cas();
            beanWrapper.setProperty(versionProperty, newCas);
          }
        } catch (final DocumentDoesNotExistException e) {
          client.insert(translateEncode, persistTo, replicateTo);
        } catch (final CASMismatchException e) {
          throw new OptimisticLockingFailureException("Saving document with version value failed: "
              + version);
        }
        return true;
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
    final BeanWrapper<Object> beanWrapper = BeanWrapper.create(objectToInsert, couchbaseConverter.getConversionService());

    final long version;
    if (persistentEntity != null && persistentEntity.hasVersionProperty()) {
      version = beanWrapper.getProperty(persistentEntity.getVersionProperty(), Long.class);
      if (version == 0) {
        beanWrapper.setProperty(persistentEntity.getVersionProperty(), 0);
      }
    } else {
      version = 0;
    }

    maybeEmitEvent(new BeforeConvertEvent<Object>(objectToInsert));
    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToInsert, converted);

    maybeEmitEvent(new BeforeSaveEvent<Object>(objectToInsert, converted));
    execute(new BucketCallback<Boolean>() {
      @Override
      public Boolean doInBucket() throws InterruptedException, ExecutionException {
        try {
          StringDocument translateEncode = translateEncode(converted, version);
          StringDocument result = client.insert(translateEncode, persistTo, replicateTo);
          if (result != null && persistentEntity.hasVersionProperty()) {
            beanWrapper.setProperty(persistentEntity.getVersionProperty(), result.cas());
          }
          return true;
        } catch (final CouchbaseException e) {
          handleWriteResultError(e, "Inserting document failed: " + e.getMessage());
          return false;
        }
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

    final BeanWrapper<Object> beanWrapper = BeanWrapper.create(objectToUpdate, couchbaseConverter.getConversionService());
    CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(objectToUpdate.getClass());
    final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();
    final long version = versionProperty != null ? beanWrapper.getProperty(versionProperty, Long.class) : 0l;

    maybeEmitEvent(new BeforeConvertEvent<Object>(objectToUpdate));
    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToUpdate, converted);

    maybeEmitEvent(new BeforeSaveEvent<Object>(objectToUpdate, converted));
    execute(new BucketCallback<StringDocument>() {
      @Override
      public StringDocument doInBucket() throws InterruptedException, ExecutionException {
        final StringDocument translateEncode = translateEncode(converted, version);
        try {
          final StringDocument document = client.replace(translateEncode, persistTo, replicateTo);
          final long newCas = document.cas();
          beanWrapper.setProperty(versionProperty, newCas);
          return document;
        } catch (final CASMismatchException e) {
          throw new OptimisticLockingFailureException("Saving document with version value failed: "
              + version);
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
      execute(new BucketCallback<StringDocument>() {
        @Override
        public StringDocument doInBucket() throws InterruptedException, ExecutionException {
          final StringDocument documentId = StringDocument.create(objectToRemove.toString());
          return client.remove(documentId, persistTo, replicateTo);
        }
      });
      maybeEmitEvent(new AfterDeleteEvent<Object>(objectToRemove));
      return;
    }

    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToRemove, converted);

    execute(new BucketCallback<StringDocument>() {
      @Override
      public StringDocument doInBucket() {
        final StringDocument documentId = StringDocument.create(converted.getId());
        return client.remove(documentId);
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
  private void handleWriteResultError(CouchbaseException e, String message) {
    if (writeResultChecking == WriteResultChecking.NONE) {
      return;
    }

    if (writeResultChecking == WriteResultChecking.EXCEPTION) {
      throw new CouchbaseDataIntegrityViolationException(message, e);
    }
    LOGGER.error(message, e);
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
  public Bucket getCouchbaseClient() {
    return client;
  }
}
