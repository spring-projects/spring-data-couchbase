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


import java.util.Collection;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.view.SpatialViewQuery;
import com.couchbase.client.java.view.SpatialViewResult;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.query.Consistency;

/**
 * Defines common operations on the Couchbase data source, most commonly implemented by {@link CouchbaseTemplate}.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 */
public interface CouchbaseOperations {

  String SELECT_ID = "_ID";
  String SELECT_CAS = "_CAS";

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
   * Save the given object.
   * <p/>
   * <p>When the document already exists (specified by its unique id), then it will be overriden. Otherwise it will be
   * created.</p>
   *
   * @param objectToSave the object to store in the bucket.
   */
  void save(Object objectToSave, PersistTo persistTo, ReplicateTo replicateTo);

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
   * Save a list of objects.
   * <p/>
   * <p>When one of the documents already exists (specified by its unique id), then it will be overriden. Otherwise it
   * will be created.</p>
   *
   * @param batchToSave the list of objects to store in the bucket.
   * @param persistTo the persistence constraint setting.
   * @param replicateTo the replication constraint setting.
   */
  void save(Collection<?> batchToSave, PersistTo persistTo, ReplicateTo replicateTo);

  /**
   * Insert the given object.
   * <p/>
   * <p>When the document already exists (specified by its unique id), then it will not be overriden. Use the
   * {@link CouchbaseOperations#save} method for this task.</p>
   *
   * @param objectToInsert the object to add to the bucket.
   */
  void insert(Object objectToInsert);

  /**
   * Insert the given object.
   * <p/>
   * <p>When the document already exists (specified by its unique id), then it will not be overriden. Use the
   * {@link CouchbaseOperations#save} method for this task.</p>
   *
   * @param objectToInsert the object to add to the bucket.
   * @param persistTo the persistence constraint setting.
   * @param replicateTo the replication constraint setting.
   */
  void insert(Object objectToInsert, PersistTo persistTo, ReplicateTo replicateTo);

  /**
   * Insert a list of objects.
   * <p/>
   * <p>When one of the documents already exists (specified by its unique id), then it will not be overriden. Use the
   * {@link CouchbaseOperations#save} method for this.</p>
   *
   * @param batchToInsert the list of objects to add to the bucket.
   */
  void insert(Collection<?> batchToInsert);

  /**
   * Insert a list of objects.
   * <p/>
   * <p>When one of the documents already exists (specified by its unique id), then it will not be overriden. Use the
   * {@link CouchbaseOperations#save} method for this.</p>
   *
   * @param batchToInsert the list of objects to add to the bucket.
   * @param persistTo the persistence constraint setting.
   * @param replicateTo the replication constraint setting.
   */
  void insert(Collection<?> batchToInsert, PersistTo persistTo, ReplicateTo replicateTo);

  /**
   * Update the given object.
   * <p/>
   * <p>When the document does not exist (specified by its unique id) it will not be created. Use the
   * {@link CouchbaseOperations#save} method for this.</p>
   *
   * @param objectToUpdate the object to add to the bucket.
   */
  void update(Object objectToUpdate);

  /**
   * Update the given object.
   * <p/>
   * <p>When the document does not exist (specified by its unique id) it will not be created. Use the
   * {@link CouchbaseOperations#save} method for this.</p>
   *
   * @param objectToUpdate the object to add to the bucket.
   * @param persistTo the persistence constraint setting.
   * @param replicateTo the replication constraint setting.
   */
  void update(Object objectToUpdate, PersistTo persistTo, ReplicateTo replicateTo);

  /**
   * Insert a list of objects.
   * <p/>
   * <p>If one of the documents does not exist (specified by its unique id), then it will not be created. Use the
   * {@link CouchbaseOperations#save} method for this.</p>
   *
   * @param batchToUpdate the list of objects to add to the bucket.
   */
  void update(Collection<?> batchToUpdate);

  /**
   * Insert a list of objects.
   * <p/>
   * <p>If one of the documents does not exist (specified by its unique id), then it will not be created. Use the
   * {@link CouchbaseOperations#save} method for this.</p>
   *
   * @param batchToUpdate the list of objects to add to the bucket.
   * @param persistTo the persistence constraint setting.
   * @param replicateTo the replication constraint setting.
   */
  void update(Collection<?> batchToUpdate, PersistTo persistTo, ReplicateTo replicateTo);

  /**
   * Find an object by its given Id and map it to the corresponding entity.
   *
   * @param id the unique ID of the document.
   * @param entityClass the entity to map to.
   * @return returns the found object or null otherwise.
   */
  <T> T findById(String id, Class<T> entityClass);

  /**
   * Query a View for a list of documents of type T.
   * <p/>
   * <p>There is no need to {@link ViewQuery#includeDocs(boolean) set includeDocs} explicitly, because it will be set to
   * true all the time. It is valid to pass in a empty constructed {@link ViewQuery} object.</p>
   * <p/>
   * <p>This method does not work with reduced views, because they by design do not contain references to original
   * objects. Use the provided {@link #queryView} method for more flexibility and direct access.</p>
   *
   * @param query the Query object (also specifying view design document and view name).
   * @param entityClass the entity to map to.
   * @return the converted collection
   */
  <T> List<T> findByView(ViewQuery query, Class<T> entityClass);


  /**
   * Query a View with direct access to the {@link ViewResult}.
   * <p>This method is available to ease the working with views by still wrapping exceptions into the Spring
   * infrastructure.</p>
   * <p>It is especially needed if you want to run reduced viewName queries, because they can't be mapped onto entities
   * directly.</p>
   *
   * @param query the Query object (also specifying view design document and view name).
   * @return ViewResult containing the results of the query.
   */
  ViewResult queryView(ViewQuery query);

  /**
   * Query a Spatial View for a list of documents of type T.
   * <p/>
   * <p>It is valid to pass in a empty constructed {@link SpatialViewQuery} object.</p>
   * <p/>
   *
   * @param query the SpatialViewQuery object (also specifying view design document and view name).
   * @param entityClass the entity to map to.
   * @return the converted collection
   */
  <T> List<T> findBySpatialView(SpatialViewQuery query, Class<T> entityClass);

  /**
   * Query a Spatial View with direct access to the {@link SpatialViewResult}.
   * <p>This method is available to ease the working with spatial views by still wrapping exceptions into the Spring
   * infrastructure.</p>
   *
   * @param query the SpatialViewQuery object (also specifying view design document and view name).
   * @return SpatialViewResult containing the results of the query.
   */
  SpatialViewResult querySpatialView(SpatialViewQuery query);

  /**
   * Query the N1QL Service for JSON data of type T. Enough data to construct the full
   * entity is expected to be selected, including the metadata {@value #SELECT_ID} and
   * {@value #SELECT_CAS} (document id and cas, obtained through N1QL's
   * "{@code META(bucket).id AS} {@value #SELECT_ID}" and
   * "{@code META(bucket).cas AS} {@value #SELECT_CAS}").
   * <p>This is done via a {@link N1qlQuery} that contains a {@link Statement} and possibly
   * additional query parameters ({@link N1qlParams}) and placeholder values if the
   * statement contains placeholders.
   * <br/>
   * Use {@link N1qlQuery}'s factory methods to construct such a Query.</p>
   *
   * @param n1ql the N1QL query.
   * @param entityClass the target class for the returned entities.
   * @param <T> the entity class
   * @return the list of entities matching this query.
   * @throws CouchbaseQueryExecutionException if the id and cas are not selected.
   */
  <T> List<T> findByN1QL(N1qlQuery n1ql, Class<T> entityClass);

  /**
   * Query the N1QL Service for partial JSON data of type T. The selected field will be
   * used in a {@link TranslationService#decodeFragment(String, Class) straightforward decoding}
   * (no document, metadata like id nor cas) to map to a "fragment class".
   * <p>This is done via a {@link N1qlQuery} that contains a {@link Statement} and possibly
   * additional query parameters ({@link N1qlParams}) and placeholder values if the
   * statement contains placeholders.
   * <br/>
   * Use {@link N1qlQuery}'s factory methods to construct such a Query.</p>
   *
   * @param n1ql the N1QL query.
   * @param fragmentClass the target class for the returned fragments.
   * @param <T> the fragment class
   * @return the list of entities matching this query.
   */
  <T> List<T> findByN1QLProjection(N1qlQuery n1ql, Class<T> fragmentClass);

  /**
   * Query the N1QL Service with direct access to the {@link N1qlQueryResult}.
   * <p>
   * This is done via a {@link N1qlQuery} that can
   * contain a {@link Statement}, additional query parameters ({@link N1qlParams})
   * and placeholder values if the statement contains placeholders.</p>
   * <p>
   * Use {@link N1qlQuery}'s factory methods to construct this.</p>
   *
   * @param n1ql the N1QL query.
   * @return {@link N1qlQueryResult} containing the results of the n1ql query.
   */
  N1qlQueryResult queryN1QL(N1qlQuery n1ql);

  /**
   * Checks if the given document exists.
   *
   * @param id the unique ID of the document.
   * @return whether the document could be found or not.
   */
  boolean exists(String id);

  /**
   * Remove the given object from the bucket by id.
   * <p/>
   * If the object is a String, it will be treated as the document key
   * directly.
   *
   * @param objectToRemove the Object to remove.
   */
  void remove(Object objectToRemove);

  /**
   * Remove the given object from the bucket by id.
   * <p/>
   * If the object is a String, it will be treated as the document key
   * directly.
   *
   * @param objectToRemove the Object to remove.
   * @param persistTo the persistence constraint setting.
   * @param replicateTo the replication constraint setting.
   */
  void remove(Object objectToRemove, PersistTo persistTo, ReplicateTo replicateTo);

  /**
   * Remove a list of objects from the bucket by id.
   *
   * @param batchToRemove the list of Objects to remove.
   */
  void remove(Collection<?> batchToRemove);

  /**
   * Remove a list of objects from the bucket by id.
   *
   * @param batchToRemove the list of Objects to remove.
   * @param persistTo the persistence constraint setting.
   * @param replicateTo the replication constraint setting.
   */
  void remove(Collection<?> batchToRemove, PersistTo persistTo, ReplicateTo replicateTo);

  /**
   * Executes a BucketCallback translating any exceptions as necessary.
   * <p/>
   * Allows for returning a result object, that is a domain object or a collection of domain objects.
   *
   * @param action the action to execute in the callback.
   * @param <T> the return type.
   * @return the return type.
   */
  <T> T execute(BucketCallback<T> action);

  /**
   * Returns the linked {@link Bucket} to this template.
   *
   * @return the client used for the template.
   */
  Bucket getCouchbaseBucket();

  /**
   * Returns the {@link ClusterInfo} about the cluster linked to this template.
   *
   * @return the info about the cluster the template connects to.
   */
  ClusterInfo getCouchbaseClusterInfo();

  /**
   * Returns the underlying {@link CouchbaseConverter}.
   *
   * @return CouchbaseConverter.
   */
  CouchbaseConverter getConverter();

  /**
   * Returns the {@link Consistency consistency} parameter to be used by default for generated queries (views and N1QL)
   * in repositories. Defaults to {@link Consistency#DEFAULT_CONSISTENCY}.
   *
   * @return the consistency to use for generated repository queries.
   */
  Consistency getDefaultConsistency();
}