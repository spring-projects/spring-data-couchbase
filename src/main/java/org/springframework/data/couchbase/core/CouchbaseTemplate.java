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

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import net.spy.memcached.internal.OperationFuture;

import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.CouchbaseStorable;
import org.springframework.data.mapping.context.MappingContext;

import com.couchbase.client.CouchbaseClient;

/**
 * @author Michael Nitschinger
 */
public class CouchbaseTemplate implements CouchbaseOperations {

  private CouchbaseClient client;
  private CouchbaseConverter couchbaseConverter;
  protected final MappingContext<? extends CouchbasePersistentEntity<?>,
      CouchbasePersistentProperty> mappingContext;
  private static final Collection<String> ITERABLE_CLASSES;
  private final CouchbaseExceptionTranslator exceptionTranslator = 
  		new CouchbaseExceptionTranslator();
  private final TranslationService<Object> translationService;
  
	static {
		Set<String> iterableClasses = new HashSet<String>();
		iterableClasses.add(List.class.getName());
		iterableClasses.add(Collection.class.getName());
		iterableClasses.add(Iterator.class.getName());
		ITERABLE_CLASSES = Collections.unmodifiableCollection(iterableClasses);
	}

  public CouchbaseTemplate(final CouchbaseClient client) {
    this(client, null);
  }

  public CouchbaseTemplate(final CouchbaseClient client,
    final CouchbaseConverter converter) {
    this.client = client;
    couchbaseConverter = converter == null ? getDefaultConverter(client) : converter;
    mappingContext = couchbaseConverter.getMappingContext();
    translationService = new JacksonTranslationService();
  }

  private CouchbaseConverter getDefaultConverter(final CouchbaseClient client) {
    MappingCouchbaseConverter converter = new MappingCouchbaseConverter(
      new CouchbaseMappingContext());
    converter.afterPropertiesSet();
    return converter;
  }

  private Object translateEncode(final CouchbaseStorable source) {
    return translationService.encode(source);
  }

  private CouchbaseStorable translateDecode(final String source, final CouchbaseStorable target) {
    return translationService.decode(source, target);
  }

  public final void insert(final Object objectToSave) {
  	ensureNotIterable(objectToSave);

    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToSave, converted);

    execute(new BucketCallback<Boolean>() {
      @Override
      public Boolean doInBucket() throws InterruptedException, ExecutionException {
        return client.add(converted.getId(), converted.getExpiration(), translateEncode(converted)).get();
      }
    });
  }
  
  public final void insert(final Collection<? extends Object> batchToSave) {
  	Iterator<? extends Object> iter = batchToSave.iterator();
  	while (iter.hasNext()) {
  		insert(iter.next());
  	}
  }

  public void save(final Object objectToSave) {
  	ensureNotIterable(objectToSave);

    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToSave, converted);

    execute(new BucketCallback<Boolean>() {
      @Override
      public Boolean doInBucket() throws InterruptedException, ExecutionException {
        return client.set(converted.getId(), converted.getExpiration(), translateEncode(converted)).get();
      }
    });
  }
  
  public void save(final Collection<? extends Object> batchToSave) {
    Iterator<? extends Object> iter = batchToSave.iterator();
    while (iter.hasNext()) {
      save(iter.next());
    }
  }

  public void update(final Object objectToSave) {
  	ensureNotIterable(objectToSave);

    final CouchbaseDocument converted = new CouchbaseDocument();
    couchbaseConverter.write(objectToSave, converted);

    execute(new BucketCallback<Boolean>() {
      @Override
      public Boolean doInBucket() throws InterruptedException, ExecutionException {
        return client.replace(converted.getId(), converted.getExpiration(), translateEncode(converted)).get();
      }
    });

  }
  
  public void update(final Collection<? extends Object> batchToSave) {
  	Iterator<? extends Object> iter = batchToSave.iterator();
  	while (iter.hasNext()) {
  		save(iter.next());
  	}
  }
  
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

    CouchbaseDocument converted = new CouchbaseDocument(id);
  	return couchbaseConverter.read(entityClass, (CouchbaseDocument) translateDecode(result, converted));
  }


  @Override
  public <T> List<T> findByView(final String designName, final String viewName,
    final Query query, final Class<T> entityClass) {

    if (!query.willIncludeDocs()) {
      query.setIncludeDocs(true);
    }
    if (query.willReduce()) {
      query.setReduce(false);
    }

    ViewResponse response = queryView(designName, viewName, query);

    List<T> result = new ArrayList<T>(response.size());
    for (ViewRow row : response) {
      CouchbaseDocument converted = new CouchbaseDocument(row.getId());
      result.add(couchbaseConverter.read(entityClass,
        (CouchbaseDocument) translateDecode((String) row.getDocument(), converted)));
    }

    return result;
  }

  @Override
  public ViewResponse queryView(final String designName, final String viewName,
    final Query query) {
    return execute(new BucketCallback<ViewResponse>() {
      @Override
      public ViewResponse doInBucket() {
        View view = client.getView(designName, viewName);
        return client.query(view, query);
      }
    });
  }

  public void remove(final Object objectToRemove) {
    ensureNotIterable(objectToRemove);

    if (objectToRemove instanceof String) {
      execute(new BucketCallback<Boolean>() {
        @Override
        public Boolean doInBucket() throws InterruptedException, ExecutionException {
          return client.delete((String) objectToRemove).get();
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

  public void remove(final Collection<? extends Object> batchToRemove) {
    Iterator<? extends Object> iter = batchToRemove.iterator();
    while (iter.hasNext()) {
      remove(iter.next());
    }
  }

  @Override
  public <T> T execute(final BucketCallback<T> action) {
    try {
      return action.doInBucket();
    } catch (RuntimeException e) {
      throw potentiallyConvertRuntimeException(e);
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
    String result = execute(new BucketCallback<String>() {
      @Override
      public String doInBucket() {
        return (String) client.get(id);
      }
    });
    return result == null ? false : true;
  }

  /**
   * Make sure the given object is not a iterable.
   *
   * @param o the object to verify.
   */
	protected final void ensureNotIterable(Object o) {
		if (null != o) {
			if (o.getClass().isArray() || ITERABLE_CLASSES.contains(o.getClass().getName())) {
				throw new IllegalArgumentException("Cannot use a collection here.");
			}
		}
	}

	private RuntimeException potentiallyConvertRuntimeException(final RuntimeException ex) {
		RuntimeException resolved = exceptionTranslator.translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}

  @Override
  public CouchbaseConverter getConverter() {
    return couchbaseConverter;
  }
}
