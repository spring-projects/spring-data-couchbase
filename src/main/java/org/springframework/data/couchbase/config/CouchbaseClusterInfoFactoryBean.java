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

package org.springframework.data.couchbase.config;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.cluster.ClusterInfo;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.CouchbaseExceptionTranslator;

/**
 * The Factory Bean to help {@link CouchbaseClusterInfoParser} constructing a {@link ClusterInfo} from a given
 * {@link Cluster} reference.
 *
 * @author Simon Baslé
 */
public class CouchbaseClusterInfoFactoryBean extends AbstractFactoryBean<ClusterInfo> implements PersistenceExceptionTranslator {

  private final Cluster cluster;
  private final String login;
  private final String password;

  private final PersistenceExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();

  public CouchbaseClusterInfoFactoryBean(Cluster cluster, String login, String password) {
    this.cluster = cluster;
    this.login = login;
    this.password = password;
  }

  @Override
  public Class<?> getObjectType() {
    return ClusterInfo.class;
  }

  @Override
  protected ClusterInfo createInstance() throws Exception {
    return cluster.clusterManager(login, password).info();
  }

  @Override
  public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
    return exceptionTranslator.translateExceptionIfPossible(ex);
  }
}
