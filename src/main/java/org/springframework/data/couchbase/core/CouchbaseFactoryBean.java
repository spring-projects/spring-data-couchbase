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
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Convenient Factory for configuring Couchbase.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseFactoryBean implements FactoryBean<CouchbaseClient>, InitializingBean,
  DisposableBean, PersistenceExceptionTranslator{

  public static final String DEFAULT_NODE = "127.0.0.1";
  public static final String DEFAULT_BUCKET = "default";
  public static final String DEFAULT_PASSWORD = "";

  private CouchbaseClient couchbaseClient;
  private PersistenceExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();
  private String bucket;
  private String password;
  private List<URI> nodes;
  private CouchbaseConnectionFactoryBuilder builder = new CouchbaseConnectionFactoryBuilder();

  public void setObservePollInterval(final int interval) {
    builder.setObsPollInterval(interval);
  }

  public void setObservePollMax(final int max) {
    builder.setObsPollMax(max);
  }

  public void setReconnectThresholdTime(final int time) {
    builder.setReconnectThresholdTime(time, TimeUnit.SECONDS);
  }

  public void setViewTimeout(final int timeout) {
    builder.setViewTimeout(timeout);
  }

  public void setFailureMode(final String mode) {
    builder.setFailureMode(FailureMode.valueOf(mode));
  }

  public void setOpTimeout(final int timeout) {
    builder.setOpTimeout(timeout);
  }

  public void setOpQueueMaxBlockTime(final int time) {
    builder.setOpQueueMaxBlockTime(time);
  }

  @Override
  public void destroy() throws Exception {
    couchbaseClient.shutdown();
  }

  @Override
  public CouchbaseClient getObject() throws Exception {
    return couchbaseClient;
  }

  @Override
  public Class<?> getObjectType() {
    return CouchbaseClient.class;
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

  public void setNodes(final URI[] nodes) {
    this.nodes = filterNonNullElementsAsList(nodes);
  }

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

  @Override
  public void afterPropertiesSet() throws Exception {
    nodes = nodes != null ? nodes : Arrays.asList(new URI("http://" + DEFAULT_NODE + ":8091/pools"));
    bucket = bucket != null ? bucket : DEFAULT_BUCKET;
    password = password != null ? password : DEFAULT_PASSWORD;

    CouchbaseConnectionFactory factory = builder.buildCouchbaseConnection(nodes, bucket, password);
    couchbaseClient = new CouchbaseClient(factory);
  }

  @Override
  public DataAccessException translateExceptionIfPossible(final RuntimeException ex) {
    return exceptionTranslator.translateExceptionIfPossible(ex);
  }
}
