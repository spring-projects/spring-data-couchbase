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

public interface CouchbaseOperations {

  /**
   * Save the given object.
   *
   * When the document already exists (specified by its unique id),
   * then it will be overriden. Otherwise it will be created.
   *
   * <p>
   * The object is converted to a JSON representation using an instance of
   * {@link CouchbaseConverter}.
   * </p>
   *
   * @param objectToSave the object to store in the bucket.
   */
  void save(Object objectToSave);
  
  /**
   * Save a list of objects.
   *
   * When one of the documents already exists (specified by its unique id),
   * then it will be overriden. Otherwise it will be created.
   *
   * @param batchToSave the list of objects to store in the bucket.
   */
  void save(Collection<? extends Object> batchToSave);	
	
  /**
   * Insert the given object.
   *
   * When the document already exists (specified by its unique id),
   * then it will not be overriden. Use the {@link CouchbaseOperations#save}
   * method for this.
   *
   * <p>
   * The object is converted to a JSON representation using an instance of
   * {@link CouchbaseConverter}.
   * </p>
   *
   * @param objectToSave the object to add to the bucket.
   */
  void insert(Object objectToSave);
  
  /**
   * Insert a list of objects.
   *
   * When one of the documents already exists (specified by its unique id),
   * then it will not be overriden. Use the {@link CouchbaseOperations#save}
   * method for this.
   *
   * @param batchToSave the list of objects to add to the bucket.
   */
  void insert(Collection<? extends Object> batchToSave);

  /**
   * Update the given object.
   *
   * When the document does not exists (specified by its unique id),
   * then it will not be created. Use the {@link CouchbaseOperations#save}
   * method for this.
   *
   * <p>
   * The object is converted to a JSON representation using an instance of
   * {@link CouchbaseConverter}.
   * </p>
   *
   * @param objectToSave the object to add to the bucket.
   */
  void update(Object objectToSave);
  
  /**
   * Insert a list of objects.
   *
   * When one of the documents does not exists (specified by its unique id),
   * then it will not be created. Use the {@link CouchbaseOperations#save}
   * method for this.
   *
   * @param batchToSave the list of objects to add to the bucket.
   */
  void update(Collection<? extends Object> batchToSave);
  
  /**
   * Find an object by its given Id and map it to the corresponding entity.
   *
   * @param id the unique ID of the document.
   * @param entityClass the entity to map to.
   * @return returns the found object or null otherwise.
   */
  <T> T findById(String id, Class<T> entityClass);

  /**
   * Remove the given object from the bucket by id.
   *
   * @param object the Object to remove.
   */
  void remove(Object object);

  /**
   * Remove a list of objects from the bucket by id.
   *
   * @param batchToRemove the list of Objects to remove.
   */
  void remove(Collection<? extends Object> batchToRemove);
}
