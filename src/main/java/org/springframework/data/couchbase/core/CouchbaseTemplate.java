/*
 * Copyright 2013 the original author or authors.
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
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.OperationFuture;
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
import org.springframework.data.mapping.context.MappingContext;

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
 */
public class CouchbaseTemplate implements CouchbaseOperations {

  private final CouchbaseClient client;
  private CouchbaseConverter couchbaseConverter;
  protected final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
  private static final Collection<String> ITERABLE_CLASSES;
  private final CouchbaseExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();
  private final TranslationService<String> translationService;

  static {
    final Set<String> iterableClasses = new HashSet<String>();
    iterableClasses.add(List.class.getName());
    iterableClasses.add(Collection.class.getName());
    iterableClasses.add(Iterator.class.getName());
    ITERABLE_CLASSES = Collections.unmodifiableCollection(iterableClasses);
  }

  public CouchbaseTemplate(final CouchbaseClient client) {
    this(client, null, null);
  }

  public CouchbaseTemplate(final CouchbaseClient client, final CouchbaseConverter couchbaseConverter, final TranslationService translationService) {
    this.client = client;
    this.couchbaseConverter = couchbaseConverter == null ? getDefaultConverter() : couchbaseConverter;
    this.translationService = translationService == null ? getDefaultTranslationService() : translationService;
    mappingContext = this.couchbaseConverter.getMappingContext();
  }

  public CouchbaseTemplate(final CouchbaseClient client, final TranslationService translationService) {
    this.client = client;
    couchbaseConverter = couchbaseConverter == null ? getDefaultConverter() : couchbaseConverter;
    this.translationService = translationService == null ? getDefaultTranslationService() : translationService;
    mappingContext = couchbaseConverter.getMappingContext();
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
    String result = execute(new BucketCallback<String>() {
      @Override
      public String doInBucket() {
        return (String) client.get(id);
      }
    });

    if (result == null) {
      return null;
    }

    final CouchbaseDocument converted = new CouchbaseDocument(id);
    return couchbaseConverter.read(entityClass, (CouchbaseDocument) translateDecode(result, converted));
  }


  @Override
  public <T> List<T> findByView(final String designName, final String viewName, final Query query, final Class<T> entityClass) {

    if (!query.willIncludeDocs()) {
      query.setIncludeDocs(true);
    }
    if (query.willReduce()) {
      query.setReduce(false);
    }

    final ViewResponse response = queryView(designName, viewName, query);

    final List<T> result = new ArrayList<T>(response.size());
    for (final ViewRow row : response) {
      final CouchbaseDocument converted = new CouchbaseDocument(row.getId());
      result.add(couchbaseConverter.read(entityClass, (CouchbaseDocument) translateDecode((String) row.getDocument(), converted)));
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

    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToSave, converted);

    execute(new BucketCallback<Boolean>() {
      @Override
      public Boolean doInBucket() throws InterruptedException, ExecutionException {
        return client.set(converted.getId(), converted.getExpiration(), translateEncode(converted), persistTo,
          replicateTo).get();
      }
    });
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

    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToInsert, converted);

    execute(new BucketCallback<Boolean>() {
      @Override
      public Boolean doInBucket() throws InterruptedException, ExecutionException {
        return client.add(converted.getId(), converted.getExpiration(), translateEncode(converted), persistTo,
          replicateTo).get();
      }
    });
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

    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToUpdate, converted);

    execute(new BucketCallback<Boolean>() {
      @Override
      public Boolean doInBucket() throws InterruptedException, ExecutionException {
        return client.replace(converted.getId(), converted.getExpiration(), translateEncode(converted), persistTo,
          replicateTo).get();
      }
    });
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

    if (objectToRemove instanceof String) {
      execute(new BucketCallback<Boolean>() {
        @Override
        public Boolean doInBucket() throws InterruptedException, ExecutionException {
          return client.delete((String) objectToRemove, persistTo, replicateTo).get();
        }
      });
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
  }

  @Override
  public void remove(Collection<?> batchToRemove, PersistTo persistTo, ReplicateTo replicateTo) {
    for (Object toRemove : batchToRemove) {
      remove(toRemove, persistTo, replicateTo);
    }
  }
}
