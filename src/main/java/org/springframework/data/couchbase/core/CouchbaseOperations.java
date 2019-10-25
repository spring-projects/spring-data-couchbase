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


import java.util.Collection;
import java.util.List;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.query.QueryResult;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.KeySettings;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.core.query.N1QLQuery;


/**
 * Defines common operations on the Couchbase data source, most commonly implemented by {@link CouchbaseTemplate}.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
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
   * Query the N1QL Service for JSON data of type T. Enough data to construct the full
   * entity is expected to be selected, including the metadata (document id and cas), obtained through N1QL's query.
   * <p>This is done via a {@link String} that contains the final query to execute.
   * <br/>
   *
   * @param n1ql the N1QL query string.
   * @param entityClass the target class for the returned entities.
   * @param <T> the entity class
   * @return the list of entities matching this query.
   * @throws CouchbaseQueryExecutionException if the id and cas are not selected.
   */
  <T> List<T> findByN1QL(N1QLQuery n1ql, Class<T> entityClass);

  /**
   * Query the N1QL Service for partial JSON data of type T. The selected field will be
   * used in a {@link TranslationService#decodeFragment(String, Class) straightforward decoding}
   * (no document, metadata like id nor cas) to map to a "fragment class".
   * <p>This is done via a {@link String} that contains represents a n1ql query.
   * <br/>
   * <p>Weak consistency in the query (eg. <code>ScanConsistency.NOT_BOUND</code>) can lead to some documents being
   * unreachable due to their deletion not having been indexed. These deleted null documents are eliminated from the
   * result of this method.</p>
   *
   * @param n1ql the N1QL query string
   * @param fragmentClass the target class for the returned fragments.
   * @param <T> the fragment class
   * @return the list of entities matching this query.
   */
  <T> List<T> findByN1QLProjection(N1QLQuery n1ql, Class<T> fragmentClass);

  /**
   * Query the N1QL Service with direct access to the {@link QueryResult}.
   * <p>
   * This is done via a {@link String} that can
   * contains the final query to execute</p>
   * <p>
   *
   * @param query the N1QL query string.
   * @return {@link QueryResult} containing the results of the n1ql query.
   */
  QueryResult queryN1QL(N1QLQuery query);

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
   * Executes a CollectionCallback translating any exceptions as necessary.
   * <p/>
   * Allows for returning a result object, that is a domain object or a collection of domain objects.
   *
   * @param action the action to execute in the callback.
   * @param <T> the return type.
   * @return the return type.
   */
  //<T> T execute(CollectionCallback<T> action);

  /**
   * Returns the linked {@link com.couchbase.client.java.Collection} to this template.
   *
   * @return the {@link com.couchbase.client.java.Collection} linked to this template.
   */
  com.couchbase.client.java.Collection getCouchbaseCollection();

  /**
   * Returns the linked {@link Bucket} for this template.
   *
   * @return the {@link Bucket} for this template
   */
  Bucket getCouchbaseBucket();

  /**
   * Returns the {@link ClusterConfig} about the cluster linked to this template.
   *
   * @return the info about the cluster the template connects to.
   */
  ClusterConfig getCouchbaseClusterConfig();

  /**
   * Returns the {@link Cluster} for this template.
   *
   * @return the {@link Cluster} this template connects to.
   */
  Cluster getCouchbaseCluster();
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

  /**
   * Add common key settings
   *
   * Throws {@link UnsupportedOperationException} if KeySettings is already set. It becomes immutable.
   *
   * @param settings {@link KeySettings}
   */
  void keySettings(final KeySettings settings);

  /**
   * Get key settings associated with the template
   *
   * @return {@link KeySettings}
   */
  KeySettings keySettings();

  /**
   * Get generated id - applies both using prefix and suffix through entity as well as template {@link KeySettings}
   *
   * ** NOTE: UNIQUE strategy will generate different ids each time ***
   *
   * @param entity the entity object.
   * @return id the couchbase document key.
   */
  String getGeneratedId(Object entity);

}
