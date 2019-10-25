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



import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.query.QueryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;

import org.springframework.data.couchbase.core.mapping.KeySettings;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.query.Consistency;

/**
 * @author Michael Nitschinger
 * @author Oliver Gierke
 * @author Simon Baslé
 * @author Young-Gu Chae
 * @author Mark Paluch
 * @author Tayeb Chlyah
 * @author David Kelly
 */
public class CouchbaseTemplate extends CouchbaseTemplateSupport implements CouchbaseOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTemplate.class);
  private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;
  private static final java.util.Collection<String> ITERABLE_CLASSES;

  static {
    final Set<String> iterableClasses = new HashSet<String>();
    iterableClasses.add(List.class.getName());
    iterableClasses.add(java.util.Collection.class.getName());
    iterableClasses.add(Iterator.class.getName());
    ITERABLE_CLASSES = java.util.Collections.unmodifiableCollection(iterableClasses);
  }

  private final Collection client;
  private final TranslationService translationService;
  private final Cluster cluster;

  private WriteResultChecking writeResultChecking = DEFAULT_WRITE_RESULT_CHECKING;
  private PersistenceExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();

  //default value is in case the template isn't constructed through configuration mechanisms that use the setter.
  private Consistency configuredConsistency = Consistency.DEFAULT_CONSISTENCY;

  public CouchbaseTemplate(final Cluster cluster, final Collection client) {
    this(cluster, client, null, null);
  }

  public CouchbaseTemplate(final Cluster cluster, final Collection client, final TranslationService translationService) {
    this(cluster, client, null, translationService);
  }

  public CouchbaseTemplate(final Cluster cluster,
                           final Collection client,
                           final CouchbaseConverter converter,
                           final TranslationService translationService) {
    this.cluster = cluster;
    this.client = client;
    this.converter = converter == null ? getDefaultConverter() : converter;
    this.translationService = translationService == null ? getDefaultTranslationService() : translationService;
    this.mappingContext = this.converter.getMappingContext();
  }


  private TranslationService getDefaultTranslationService() {
    JacksonTranslationService t = new JacksonTranslationService();
    t.afterPropertiesSet();
    return t;
  }

  private CouchbaseConverter getDefaultConverter() {
    MappingCouchbaseConverter c = new MappingCouchbaseConverter(new CouchbaseMappingContext());
    c.afterPropertiesSet();
    return c;
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

  /**
   * Handle write errors according to the set {@link #writeResultChecking} setting.
   *
   * @param message the message to use.
   */
  private void handleWriteResultError(String message, Exception cause) {
    if (writeResultChecking == WriteResultChecking.NONE) {
      return;
    }

    if (writeResultChecking == WriteResultChecking.EXCEPTION) {
      throw new CouchbaseDataIntegrityViolationException(message, cause);
    }
    else {
      LOGGER.error(message, cause);
    }
  }

  /**
   * Configures the WriteResultChecking to be used with the template. Setting null will reset
   * the default of DEFAULT_WRITE_RESULT_CHECKING. This can be configured to capture couchbase
   * specific exceptions like Temporary failure, Authentication failure..
   *
   * @param writeResultChecking the setting to use.
   */
  public void setWriteResultChecking(WriteResultChecking writeResultChecking) {
    this.writeResultChecking = writeResultChecking == null ? DEFAULT_WRITE_RESULT_CHECKING : writeResultChecking;
  }

  @Override
  public void save(Object objectToSave) {
    save(objectToSave, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void save(Object objectToSave, PersistTo persistTo, ReplicateTo replicateTo) {
    doPersist(client.reactive(), objectToSave, PersistType.SAVE,  persistTo, replicateTo).block();
  }

  @Override
  public void save(java.util.Collection<?> batchToSave) {
    save(batchToSave, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void save(java.util.Collection<?> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
    // TODO: maybe we can be more efficient?
    for (Object o : batchToSave) {
      doPersist(client.reactive(), o, PersistType.SAVE, persistTo, replicateTo).block();
    }
  }

  @Override
  public void insert(Object objectToInsert) {
    insert(objectToInsert, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void insert(Object objectToInsert, PersistTo persistTo, ReplicateTo replicateTo) {
    doPersist(client.reactive(), objectToInsert, PersistType.INSERT, persistTo, replicateTo).block();
  }

  @Override
  public void insert(java.util.Collection<?> batchToInsert) {
    insert(batchToInsert, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void insert(java.util.Collection<?> batchToInsert, PersistTo persistTo, ReplicateTo replicateTo) {
    for (Object o : batchToInsert) {
      doPersist(client.reactive(), o, PersistType.INSERT, persistTo, replicateTo).block();
    }
  }

  @Override
  public void update(Object objectToUpdate) {
    update(objectToUpdate, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void update(Object objectToUpdate, PersistTo persistTo, ReplicateTo replicateTo) {
    doPersist(client.reactive(), objectToUpdate, PersistType.UPDATE, persistTo, replicateTo).block();
  }

  @Override
  public void update(java.util.Collection<?> batchToUpdate) {
    update(batchToUpdate, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void update(java.util.Collection<?> batchToUpdate, PersistTo persistTo, ReplicateTo replicateTo) {
    // TODO: perhaps we can be more efficient?
    for (Object o : batchToUpdate) {
      doPersist(client.reactive(), o, PersistType.UPDATE, persistTo, replicateTo).block();
    }
  }

  @Override
  public <T> T findById(final String id, Class<T> entityClass) {
    return doFind(client.reactive(), id, entityClass).block();
  }


  @Override
  public <T> List<T> findByN1QL(N1QLQuery n1ql, Class<T> entityClass) {
   return doFindByN1QL(cluster.reactive(), n1ql, entityClass).collectList().block();
  }

  @Override
  public QueryResult queryN1QL(N1QLQuery query) {
      return doQueryN1QL(cluster, query);
  }

  @Override
  public <T> List<T> findByN1QLProjection(N1QLQuery n1ql, Class<T> entityClass) {
     // TODO: fill in the error handling properly
    return doQueryN1QL(cluster.reactive(), n1ql)
            .flatMapMany(res -> res.rowsAs(entityClass))
            .collectList()
            .block();
  }

  @Override
  public boolean exists(final String id) {
    return doExists(client.reactive(), id).block();
  }

  @Override
  public void remove(Object objectToRemove) {
    remove(objectToRemove, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void remove(Object objectToRemove, PersistTo persistTo, ReplicateTo replicateTo) {
    doRemove(client.reactive(), objectToRemove, persistTo, replicateTo).block();
  }

  @Override
  public void remove(java.util.Collection<?> batchToRemove) {
    remove(batchToRemove, PersistTo.NONE, ReplicateTo.NONE);
  }

  @Override
  public void remove(java.util.Collection<?> batchToRemove, PersistTo persistTo, ReplicateTo replicateTo) {
    for (Object o : batchToRemove) {
      doRemove(client.reactive(), o, persistTo, replicateTo).block();
    }
  }

  @Override
  public Bucket getCouchbaseBucket() {
    return cluster.bucket(client.bucketName());
  }

  @Override
  public CouchbaseConverter getConverter() {
    return this.converter;
  }

  @Override
  public ClusterConfig getCouchbaseClusterConfig() { return cluster.core().clusterConfig(); }

  @Override
  public Collection getCouchbaseCollection() { return client; }

  @Override
  public Cluster getCouchbaseCluster() { return cluster; }

  @Override
  public Consistency getDefaultConsistency() {
    return configuredConsistency;
  }

  public void setDefaultConsistency(Consistency consistency) {
    this.configuredConsistency = consistency;
  }


  @Override
  public void keySettings(KeySettings settings) {
    if (this.keySettings != null) {
      throw new UnsupportedOperationException("Key settings is already set, it is no longer mutable");
    }
    this.keySettings = settings;
  }

  @Override
  public KeySettings keySettings() {
    return this.keySettings;
  }

  @Override
  public String getGeneratedId(Object entity) {
    ensureNotIterable(entity);
    CouchbaseDocument converted = new CouchbaseDocument();
    converter.write(entity, converted);
    return addCommonPrefixAndSuffix(converted.getId());
  }

  private String addCommonPrefixAndSuffix(final String id) {
    String convertedKey = id;
    if (this.keySettings == null) {
      return id;
    }
    String prefix = this.keySettings.prefix();
    String delimiter = this.keySettings.delimiter();
    String suffix = this.keySettings.suffix();
    if (prefix != null && !prefix.equals("")) {
      convertedKey = prefix + delimiter + convertedKey;
    }
    if (suffix != null && !suffix.equals("")) {
      convertedKey = convertedKey + delimiter + suffix;
    }
    return convertedKey;
  }
}
