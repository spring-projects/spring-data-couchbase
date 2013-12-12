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

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Convenient Factory for configuring a {@link CouchbaseClient}.
 *
 * To set the properties correctly on the {@link CouchbaseClient} a {@link CouchbaseConnectionFactoryBuilder} is used.
 * After all properties are set, the client is constructed and used.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseFactoryBean implements FactoryBean<CouchbaseClient>, InitializingBean,
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
  private CouchbaseClient couchbaseClient;

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
  private List<URI> nodes;

  /**
   * The builder which allows to customize client settings.
   */
  private final CouchbaseConnectionFactoryBuilder builder = new CouchbaseConnectionFactoryBuilder();

  /**
   * Set the observe poll interval in miliseconds.
   *
   * @param interval the observe poll interval.
   */
  public void setObservePollInterval(final int interval) {
    builder.setObsPollInterval(interval);
  }

  /**
   * Set the maximum number of polls.
   *
   * @param max the maximum number of polls.
   */
  public void setObservePollMax(final int max) {
    builder.setObsPollMax(max);
  }

  /**
   * Set the reconnect threshold time in seconds.
   *
   * @param time the reconnect threshold time.
   */
  public void setReconnectThresholdTime(final int time) {
    builder.setReconnectThresholdTime(time, TimeUnit.SECONDS);
  }

  /**
   * Set the view timeout in miliseconds.
   *
   * @param timeout the view timeout.
   */
  public void setViewTimeout(final int timeout) {
    builder.setViewTimeout(timeout);
  }

  /**
   * Set the failure mode if memcached buckets are used.
   *
   * See the proper values of {@link FailureMode} to use.
   *
   * @param mode the failure mode.
   */
  public void setFailureMode(final String mode) {
    builder.setFailureMode(FailureMode.valueOf(mode));
  }

  /**
   * Set the operation timeout in miliseconds.
   *
   * @param timeout the operation timeout.
   */
  public void setOpTimeout(final int timeout) {
    builder.setOpTimeout(timeout);
  }

  /**
   * Set the operation queue maximum block time in miliseconds.
   *
   * @param time the operation queue maximum block time.
   */
  public void setOpQueueMaxBlockTime(final int time) {
    builder.setOpQueueMaxBlockTime(time);
  }

  /**
   * Shutdown the client when the bean is destroyed.
   *
   * @throws Exception if shutdown failed.
   */
  @Override
  public void destroy() throws Exception {
    couchbaseClient.shutdown();
  }

  /**
   * Return the underlying {@link CouchbaseClient}.
   *
   * @return the client object.
   * @throws Exception if returning the client failed.
   */
  @Override
  public CouchbaseClient getObject() throws Exception {
    return couchbaseClient;
  }

  /**
   * Returns the object type of the client.
   *
   * @return the {@link CouchbaseClient} class.
   */
  @Override
  public Class<?> getObjectType() {
    return CouchbaseClient.class;
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
    this.nodes = convertHosts(hosts);
  }

  /**
   * Convert a list of hosts into a URI format that can be used by the {@link CouchbaseClient}.
   *
   * To make it simple to use, the list of hosts can be passed in as a comma separated list. This list gets parsed
   * and converted into a URI format that is suitable for the underlying {@link CouchbaseClient} object.
   *
   * @param hosts the host list to convert.
   * @return the converted list with URIs.
   */
  private List<URI> convertHosts(final String hosts) {
    String[] split = hosts.split(",");
    List<URI> nodes = new ArrayList<URI>();

    try {
      for (int i = 0; i < split.length; i++) {
        nodes.add(new URI("http://" + split[i] + ":8091/pools"));
      }
    } catch (URISyntaxException ex) {
      throw new BeanCreationException("Could not convert host list." + ex);
    }

    return nodes;
  }

  /**
   * Set the nodes as an array of URIs.
   *
   * @param nodes the nodes to connect to.
   */
  public void setNodes(final URI[] nodes) {
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
    for (T element : elements) {
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
    nodes = nodes != null ? nodes : Arrays.asList(new URI("http://" + DEFAULT_NODE + ":8091/pools"));
    bucket = bucket != null ? bucket : DEFAULT_BUCKET;
    password = password != null ? password : DEFAULT_PASSWORD;

    CouchbaseConnectionFactory factory = builder.buildCouchbaseConnection(nodes, bucket, password);
    couchbaseClient = new CouchbaseClient(factory);
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
