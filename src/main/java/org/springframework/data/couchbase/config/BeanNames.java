/*
 * Copyright 2012-2017 the original author or authors
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

package org.springframework.data.couchbase.config;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.env.CouchbaseEnvironment;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;

/**
 * Contains default bean names for Couchbase beans.
 *
 * These are the names of the beans used by Spring Data Couchbase, unless an explicit id is given to the bean
 * either in the xml configuration or the {@link AbstractCouchbaseConfiguration java configuration}.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 */
public class BeanNames {

  /**
   * The name for the default {@link CouchbaseEnvironment} bean.
   *
   * See {@link AbstractCouchbaseConfiguration#couchbaseEnvironment()} for java config, and
   * the "&lt;couchbase:env /&gt;" element for xml config.
   */
  public static final String COUCHBASE_ENV = "couchbaseEnv";

  /**
   * The name for the default {@link Cluster} bean.
   *
   * See {@link AbstractCouchbaseConfiguration#couchbaseCluster()} for java config, and
   * the "&lt;couchbase:cluster /&gt;" element for xml config.
   */
  public static final String COUCHBASE_CLUSTER = "couchbaseCluster";

  /**
   * The name for the default {@link Bucket} bean.
   *
   * See {@link AbstractCouchbaseConfiguration#couchbaseClient()} for java config, and
   * the "&lt;couchbase:bucket /&gt;" element for xml config.
   */
  public static final String COUCHBASE_BUCKET = "couchbaseBucket";

  /**
   * The name for the default {@link CouchbaseOperations} bean.
   *
   * See {@link AbstractCouchbaseConfiguration#couchbaseTemplate()} for java config, and
   * the "&lt;couchbase:template /&gt;" element for xml config.
   */
  public static final String COUCHBASE_TEMPLATE = "couchbaseTemplate";

  /**
   * The name for the default {@link TranslationService} bean.
   *
   * See {@link AbstractCouchbaseConfiguration#translationService()} for java config, and
   * the "&lt;couchbase:translation-service /&gt;" element for xml config.
   */
  public static final String COUCHBASE_TRANSLATION_SERVICE = "couchbaseTranslationService";

  /**
   * The name for the default {@link ClusterInfo} bean.
   *
   * See {@link AbstractCouchbaseConfiguration#couchbaseClusterInfo()} for java config, and
   * the "&lt;couchbase:clusterInfo /&gt;" element for xml config.
   */
  public static final String COUCHBASE_CLUSTER_INFO = "couchbaseClusterInfo";

  /**
   * The name for the bean that stores custom mapping between repositories and their backing couchbaseOperations.
   */
  public static final String COUCHBASE_OPERATIONS_MAPPING = "couchbaseRepositoryOperationsMapping";

  /**
   * The name for the bean that stores custom mapping between reactive repositories and their backing reactiveCouchbaseOperations.
   */
  public static final String REACTIVE_COUCHBASE_OPERATIONS_MAPPING = "reactiveCouchbaseRepositoryOperationsMapping";

  /**
   * The name for the bean that stores custom mapping between rxjava repositories and their backing rxjavaCouchbaseOperations.
   */
  public static final String RXJAVA_COUCHBASE_OPERATIONS_MAPPING = "rxJavaCouchbaseRepositoryOperationsMapping";

  /**
   * The name for the  bean that drives how some indexes are automatically created.
   */
  public static final String COUCHBASE_INDEX_MANAGER = "couchbaseIndexManager";

  /**
   * The name for the  bean that performs conversion to/from representation suitable for storage in couchbase.
   */
  public static final String COUCHBASE_MAPPING_CONVERTER = "couchbaseMappingConverter";

  /**
   * The name for the  bean that stores mapping metadata for entities stored in couchbase.
   */
  public static final String COUCHBASE_MAPPING_CONTEXT = "couchbaseMappingContext";

  /**
   * The name for the  bean that registers custom {@link Converter Converters} to encode/decode entity members.
   */
  public static final String COUCHBASE_CUSTOM_CONVERSIONS = "couchbaseCustomConversions";

  /**
   * The name for the bean that will handle audit trail marking of entities.
   */
  public static final String COUCHBASE_AUDITING_HANDLER = "couchbaseAuditingHandler";

  /**
   * The name for the default {@link ReactiveCouchbaseOperations} bean.
   *
   * See {@link AbstractReactiveCouchbaseConfiguration#reactiveCouchbaseTemplate()} for java config, and
   * the "&lt;couchbase:template /&gt;" element for xml config.
   */
  public static final String REACTIVE_COUCHBASE_TEMPLATE = "reactiveCouchbaseTemplate";

  /**
   * The name for the default {@link RxJavaCouchbaseOperations} bean.
   *
   * See {@link AbstractRxJavaCouchbaseConfiguration#rxjava1CouchbaseTemplate()} for java config, and
   * the "&lt;couchbase:template /&gt;" element for xml config.
   */
  public static final String RXJAVA1_COUCHBASE_TEMPLATE = "rxjava1CouchbaseTemplate";


}
