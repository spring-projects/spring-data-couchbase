/**
 * Copyright (C) 2009-2012 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.spring.core;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.spy.memcached.internal.OperationFuture;
import org.springframework.data.mapping.context.MappingContext;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.spring.core.convert.CouchbaseConverter;
import com.couchbase.spring.core.convert.MappingCouchbaseConverter;
import com.couchbase.spring.core.mapping.ConvertedCouchbaseDocument;
import com.couchbase.spring.core.mapping.CouchbasePersistentEntity;
import com.couchbase.spring.core.mapping.CouchbasePersistentProperty;

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

  public void remove(final Object objectToRemove) {
    ensureNotIterable(objectToRemove);

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

  
}
