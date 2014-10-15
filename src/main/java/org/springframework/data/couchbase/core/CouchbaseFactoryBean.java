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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

/**
 * Convenient Factory for configuring a {@link CouchbaseClient}.
 *
 * To set the properties correctly on the {@link CouchbaseClient} a {@link CouchbaseConnectionFactoryBuilder} is used.
 * After all properties are set, the client is constructed and used.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseFactoryBean implements FactoryBean<Bucket>, InitializingBean,
  DisposableBean, PersistenceExceptionTranslator {

  /**
   * Defines the default hostname to be used if no other list is supplied.
   */
  public static final String DEFAULT_NODE = "127.0.0.1";

  /**
   * Defines the default bucket name to be used if no other bucket name is supplied.
   */
  public static final String DEFAULT_BUCKET = "default";

  /**
   * Defines the password of the default bucket.
   */
  public static final String DEFAULT_PASSWORD = "";

  /**
   * The name of the default shutdown method to call when the context is destroyed.
   */
  public static final String DEFAULT_DESTROY_METHOD = "shutdown";

  /**
   * Use SLF4J as the default logger if not instructed otherwise.
   */
  public static final String DEFAULT_LOGGER_PROPERTY = "net.spy.memcached.compat.log.SLF4JLogger";

  /**
   * Holds the enclosed {@link CouchbaseClient}.
   */
  private Bucket couchbaseClient;

  /**
   * The exception translator is used to properly map exceptions to spring-type exceptions.
   */
  private PersistenceExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();

  /**
   * Contains the actual bucket name.
   */
  private String bucket;

  /**
   * Contains the actual bucket password.
   */
  private String password;

  /**
   * Contains the list of nodes to connect to.
   */
  private List<String> nodes;

  /**
   * The builder which allows to customize client settings.
   */
  private final DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder();

  /**
   * Shutdown the client when the bean is destroyed.
   *
   * @throws Exception if shutdown failed.
   */
  @Override
  public void destroy() throws Exception {
    couchbaseClient.close();
  }

  /**
   * Return the underlying {@link CouchbaseClient}.
   *
   * @return the client object.
   * @throws Exception if returning the client failed.
   */
  @Override
  public Bucket getObject() throws Exception {
    return couchbaseClient;
  }

  /**
   * Returns the object type of the client.
   *
   * @return the {@link CouchbaseClient} class.
   */
  @Override
  public Class<?> getObjectType() {
    return Bucket.class;
  }

  /**
   * The client should be returned as a singleton.
   *
   * @return returns true.
   */
  @Override
  public boolean isSingleton() {
    return true;
  }

  /**
   * Set the bucket to be used.
   *
   * @param bucket the bucket to use.
   */
  public void setBucket(final String bucket) {
    this.bucket = bucket;
  }

  /**
   * Set the password.
   *
   * @param password the password to use.
   */
  public void setPassword(final String password) {
    this.password = password;
  }

  /**
   * Set the array of nodes from a delimited list of hosts.
   *
   * @param hosts a comma separated list of hosts.
   */
  public void setHost(final String hosts) {
    this.nodes = filterNonNullElementsAsList(hosts.split(","));
  }

  /**
   * Set the nodes as an array of URIs.
   *
   * @param nodes the nodes to connect to.
   */
  public void setNodes(final String[] nodes) {
    this.nodes = filterNonNullElementsAsList(nodes);
  }

  /**
   * Convert the array of elements to a list and filter empty or null elements.
   *
   * @param elements the elements to convert.
   * @param <T> the type of the elements.
   * @return the converted list.
   */
  private <T> List<T> filterNonNullElementsAsList(T[] elements) {
    if (elements == null) {
      return Collections.emptyList();
    }

    List<T> candidateElements = new ArrayList<T>();
    for (final T element : elements) {
      if (element != null) {
        candidateElements.add(element);
      }
    }

    return Collections.unmodifiableList(candidateElements);
  }

  /**
   * Instantiate the {@link CouchbaseClient}.
   *
   * @throws Exception if something goes wrong during instantiation.
   */
  @Override
  public void afterPropertiesSet() throws Exception {
    bucket = bucket != null ? bucket : DEFAULT_BUCKET;
    password = password != null ? password : DEFAULT_PASSWORD;
    nodes = nodes != null ? nodes : Arrays.asList(DEFAULT_NODE);

    final DefaultCouchbaseEnvironment factory = builder.build();
    final CouchbaseCluster cluster = CouchbaseCluster.create(factory, nodes);
    couchbaseClient = cluster.openBucket(bucket, password);
  }

  /**
   * Translate exception if possible.
   *
   * @param ex the exception to translate.
   * @return the translate exception.
   */
  @Override
  public DataAccessException translateExceptionIfPossible(final RuntimeException ex) {
    return exceptionTranslator.translateExceptionIfPossible(ex);
  }

}
