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

import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import net.spy.memcached.internal.OperationFuture;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.ConvertedCouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
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
    this.couchbaseConverter = converter == null ? getDefaultConverter(client) : converter;
    this.mappingContext = this.couchbaseConverter.getMappingContext();
  }

  private CouchbaseConverter getDefaultConverter(final CouchbaseClient client) {
    MappingCouchbaseConverter converter = new MappingCouchbaseConverter(
      new CouchbaseMappingContext());
    converter.afterPropertiesSet();
    return converter;
  }

  public final void insert(final Object objectToSave) {
  	ensureNotIterable(objectToSave);

    final ConvertedCouchbaseDocument converted =
      new ConvertedCouchbaseDocument();
    couchbaseConverter.write(objectToSave, converted);
    execute(new BucketCallback<OperationFuture<Boolean>>() {
      @Override
      public OperationFuture<Boolean> doInBucket() {
        return client.add(
          converted.getId(), converted.getExpiry(), converted.getRawValue());
      }
    });
  }
  
  public final void insert(final Collection<? extends Object> batchToSave) {
  	Iterator<? extends Object> iter = batchToSave.iterator();
  	while(iter.hasNext()) {
  		insert(iter.next());
  	}
  }

  public void save(final Object objectToSave) {
  	ensureNotIterable(objectToSave);

    final ConvertedCouchbaseDocument converted =
      new ConvertedCouchbaseDocument();
    couchbaseConverter.write(objectToSave, converted);

    execute(new BucketCallback<OperationFuture<Boolean>>() {
      @Override
      public OperationFuture<Boolean> doInBucket() {
        return client.set(
          converted.getId(), converted.getExpiry(), converted.getRawValue());
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

    final ConvertedCouchbaseDocument converted =
      new ConvertedCouchbaseDocument();
    couchbaseConverter.write(objectToSave, converted);

    execute(new BucketCallback<OperationFuture<Boolean>>() {
      @Override
      public OperationFuture<Boolean> doInBucket() {
        return client.replace(
          converted.getId(), converted.getExpiry(), converted.getRawValue());
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
  	
  	ConvertedCouchbaseDocument converted = new ConvertedCouchbaseDocument(id, result);
  	return couchbaseConverter.read(entityClass, converted);
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
      ConvertedCouchbaseDocument converted =
        new ConvertedCouchbaseDocument(row.getId(), (String) row.getDocument());
      result.add(couchbaseConverter.read(entityClass, converted));
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
      execute(new BucketCallback<OperationFuture<Boolean>>() {
        @Override
        public OperationFuture<Boolean> doInBucket() {
          return client.delete((String) objectToRemove);
        }
      });
      return;
    }

    final ConvertedCouchbaseDocument converted = new ConvertedCouchbaseDocument();
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

  public <T> T execute(final BucketCallback<T> action) {
    try {
      return action.doInBucket();
    } catch (RuntimeException e) {
      throw potentiallyConvertRuntimeException(e);
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
		RuntimeException resolved = this.exceptionTranslator.translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}

  @Override
  public CouchbaseConverter getConverter() {
    return couchbaseConverter;
  }
}
