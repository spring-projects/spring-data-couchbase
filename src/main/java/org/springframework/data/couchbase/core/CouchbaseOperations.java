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


import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.ViewResponse;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;

import java.util.Collection;
import java.util.List;

/**
 * Defines common operations on the Couchbase data source, most commonly implemented by {@link CouchbaseTemplate}.
 *
 * @author Michael Nitschinger
 */
public interface CouchbaseOperations {

  /**
   * Save the given object.
   * <p/>
   * <p>When the document already exists (specified by its unique id), then it will be overriden. Otherwise it will be
   * created.</p>
   *
   * @param objectToSave the object to store in the bucket.
   */
  void save(Object objectToSave);

  /**
   * Save a list of objects.
   * <p/>
   * <p>When one of the documents already exists (specified by its unique id), then it will be overriden. Otherwise it
   * will be created.</p>
   *
   * @param batchToSave the list of objects to store in the bucket.
   */
  void save(Collection<?> batchToSave);

  /**
   * Insert the given object.
   * <p/>
   * <p>When the document already exists (specified by its unique id), then it will not be overriden. Use the
   * {@link CouchbaseOperations#save} method for this task.</p>
   *
   * @param objectToSave the object to add to the bucket.
   */
  void insert(Object objectToSave);

  /**
   * Insert a list of objects.
   * <p/>
   * <p>When one of the documents already exists (specified by its unique id), then it will not be overriden. Use the
   * {@link CouchbaseOperations#save} method for this.</p>
   *
   * @param batchToSave the list of objects to add to the bucket.
   */
  void insert(Collection<?> batchToSave);

  /**
   * Update the given object.
   * <p/>
   * <p>When the document does not exist (specified by its unique id) it will not be created. Use the
   * {@link CouchbaseOperations#save} method for this.</p>
   *
   * @param objectToSave the object to add to the bucket.
   */
  void update(Object objectToSave);

  /**
   * Insert a list of objects.
   * <p/>
   * <p>If one of the documents does not exist (specified by its unique id), then it will not be created. Use the
   * {@link CouchbaseOperations#save} method for this.</p>
   *
   * @param batchToSave the list of objects to add to the bucket.
   */
  void update(Collection<?> batchToSave);

  /**
   * Find an object by its given Id and map it to the corresponding entity.
   *
   * @param id the unique ID of the document.
   * @param entityClass the entity to map to.
   *
   * @return returns the found object or null otherwise.
   */
  <T> T findById(String id, Class<T> entityClass);

  /**
   * Query a View for a list of documents of type T.
   * <p/>
   * <p>There is no need to {@link Query#setIncludeDocs(boolean)} explicitely, because it will be set to true all the
   * time. It is valid to pass in a empty constructed {@link Query} object.</p>
   * <p/>
   * <p>This method does not work with reduced views, because they by design do not contain references to original
   * objects. Use the provided {@link #queryView} method for more flexibility and direct access.</p>
   *
   * @param design the name of the design document.
   * @param view the name of the view.
   * @param query the Query object to customize the view query.
   * @param entityClass the entity to map to.
   *
   * @return the converted collection
   */
  <T> List<T> findByView(String design, String view, Query query, Class<T> entityClass);


  /**
   * Query a View with direct access to the {@link ViewResponse}.
   * <p/>
   * <p>This method is available to ease the working with views by still wrapping exceptions into the Spring
   * infrastructure.</p>
   * <p/>
   * <p>It is especially needed if you want to run reduced viewName queries, because they can't be mapped onto entities
   * directly.</p>
   *
   * @param design the name of the designDocument document.
   * @param view the name of the viewName.
   * @param query the Query object to customize the viewName query.
   *
   * @return ViewResponse containing the results of the query.
   */
  ViewResponse queryView(String design, String view, Query query);

  /**
   * Checks if the given document exists.
   *
   * @param id the unique ID of the document.
   *
   * @return whether the document could be found or not.
   */
  boolean exists(String id);

  /**
   * Remove the given object from the bucket by id.
   * <p/>
   * If the object is a String, it will be treated as the document key
   * directly.
   *
   * @param object the Object to remove.
   */
  void remove(Object object);

  /**
   * Remove a list of objects from the bucket by id.
   *
   * @param batchToRemove the list of Objects to remove.
   */
  void remove(Collection<?> batchToRemove);

  /**
   * Executes a BucketCallback translating any exceptions as necessary.
   * <p/>
   * Allows for returning a result object, that is a domain object or a collection of domain objects.
   *
   * @param action the action to execute in the callback.
   * @param <T> the return type.
   *
   * @return the return type.
   */
  <T> T execute(BucketCallback<T> action);

  /**
   * Returns the underlying {@link CouchbaseConverter}.
   *
   * @return CouchbaseConverter.
   */
  CouchbaseConverter getConverter();

}
