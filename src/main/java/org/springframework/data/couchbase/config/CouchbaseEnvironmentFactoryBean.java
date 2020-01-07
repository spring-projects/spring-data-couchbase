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

package org.springframework.data.couchbase.config;

import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.env.ClusterEnvironment.Builder;

import org.springframework.beans.factory.config.AbstractFactoryBean;

import java.time.Duration;

/**
 * Factory Bean to help create a CouchbaseEnvironment (by offering setters for supported tuning methods).
 *
 * @author Simon Baslé
 * @author Simon Bland
 * @author Subhashni Balakrishnan
 */
/*package*/ class CouchbaseEnvironmentFactoryBean extends AbstractFactoryBean<ClusterEnvironment> {

  public static final String RETRYSTRATEGY_FAILFAST = "FailFast";
  public static final String RETRYSTRATEGY_BESTEFFORT = "BestEffort";
  
  private final Builder couchbaseEnvBuilder = ClusterEnvironment.builder();
  /*
  These are tunings that are not practical to be exposed in a xml configuration
  or not supposed to be modified that easily:
    observeIntervalDelay
    reconnectDelay
    retryDelay
    userAgent
    packageNameAndVersion
    ioPool
    scheduler
    eventBus
    systemMetricsCollectorConfig
    networkLatencyMetricsCollectorConfig
    requestBufferWaitStrategy
    sslKeystore
    memcachedHashingStrategy
    kvIoPool
    queryIoPool
    searchIoPool
    viewIoPool
    kvServiceConfig
    queryServiceConfig
    searchServiceConfig
    viewServiceConfig
    cryptoManager
    tracer
    orphanResponseReporter
   */

  @Override
  public Class<?> getObjectType() {
    return ClusterEnvironment.class;
  }

  @Override
  protected ClusterEnvironment createInstance() throws Exception {
    return couchbaseEnvBuilder.build();
  }

  /**
   * Sets the {@link RetryStrategy} to use from an enum-like String value.
   * Either "FailFast" or "BestEffort" are recognized.
   *
   * @param retryStrategy the string value enum from which to choose a strategy.
   */
  public void setRetryStrategy(String retryStrategy) {
    if (RETRYSTRATEGY_FAILFAST.equals(retryStrategy)) {
      this.couchbaseEnvBuilder.retryStrategy(FailFastRetryStrategy.INSTANCE);
    } else if (RETRYSTRATEGY_BESTEFFORT.equals(retryStrategy)) {
      this.couchbaseEnvBuilder.retryStrategy(BestEffortRetryStrategy.INSTANCE);
    }
  }

  //==== SETTERS for the factory bean ====

  public void setManagementTimeout(long managementTimeout) {
    couchbaseEnvBuilder.timeoutConfig().managementTimeout(Duration.ofMillis(managementTimeout));
  }

  public void setQueryTimeout(long queryTimeout) {
    this.couchbaseEnvBuilder.timeoutConfig().queryTimeout(Duration.ofMillis(queryTimeout));
  }

  public void setViewTimeout(long viewTimeout) {
    this.couchbaseEnvBuilder.timeoutConfig().viewTimeout(Duration.ofMillis(viewTimeout));
  }

  public void setKvTimeout(long kvTimeout) {
    this.couchbaseEnvBuilder.timeoutConfig().kvTimeout(Duration.ofMillis(kvTimeout));
  }

  public void setConnectTimeout(long connectTimeout) {
    this.couchbaseEnvBuilder.timeoutConfig().connectTimeout(Duration.ofMillis(connectTimeout));
  }

  public void setDisconnectTimeout(long disconnectTimeout) {
    this.couchbaseEnvBuilder.timeoutConfig().disconnectTimeout(Duration.ofMillis(disconnectTimeout));
  }

  public void setEnableDnsSrv(boolean dnsSrvEnabled) {
    this.couchbaseEnvBuilder.ioConfig().enableDnsSrv(dnsSrvEnabled);
  }

  /*public void setSslEnabled(boolean sslEnabled) {
    this.couchbaseEnvBuilder.sslEnabled(sslEnabled);
  }

  public void setSslKeystoreFile(String sslKeystoreFile) {
    this.couchbaseEnvBuilder.sslKeystoreFile(sslKeystoreFile);
  }

  public void setSslKeystorePassword(String sslKeystorePassword) {
    this.couchbaseEnvBuilder.sslKeystorePassword(sslKeystorePassword);
  }

  public void setBootstrapHttpEnabled(boolean bootstrapHttpEnabled) {
    this.couchbaseEnvBuilder.bootstrapHttpEnabled(bootstrapHttpEnabled);
  }

  public void setBootstrapCarrierEnabled(boolean bootstrapCarrierEnabled) {
    this.couchbaseEnvBuilder.bootstrapCarrierEnabled(bootstrapCarrierEnabled);
  }

  public void setBootstrapHttpDirectPort(int bootstrapHttpDirectPort) {
    this.couchbaseEnvBuilder.bootstrapHttpDirectPort(bootstrapHttpDirectPort);
  }

  public void setBootstrapHttpSslPort(int bootstrapHttpSslPort) {
    this.couchbaseEnvBuilder.bootstrapHttpSslPort(bootstrapHttpSslPort);
  }

  public void setBootstrapCarrierDirectPort(int bootstrapCarrierDirectPort) {
    this.couchbaseEnvBuilder.bootstrapCarrierDirectPort(bootstrapCarrierDirectPort);
  }

  public void setBootstrapCarrierSslPort(int bootstrapCarrierSslPort) {
    this.couchbaseEnvBuilder.bootstrapCarrierSslPort(bootstrapCarrierSslPort);
  }

  public void setIoPoolSize(int ioPoolSize) {
    this.couchbaseEnvBuilder.ioPoolSize(ioPoolSize);
  }

  public void setComputationPoolSize(int computationPoolSize) {
    this.couchbaseEnvBuilder.computationPoolSize(computationPoolSize);
  }

  public void setResponseBufferSize(int responseBufferSize) {
    this.couchbaseEnvBuilder.responseBufferSize(responseBufferSize);
  }

  public void setRequestBufferSize(int requestBufferSize) {
    this.couchbaseEnvBuilder.requestBufferSize(requestBufferSize);
  }

  public void setKvEndpoints(int kvEndpoints) {
    this.couchbaseEnvBuilder.serviceConfig().keyValueServiceConfig().endpoints(kvEndpoints);
  }

  public void setViewMinEndpoints(int minViewEndpoints) {
    this.couchbaseEnvBuilder.serviceConfig().viewServiceConfig().minEndpoints(minViewEndpoints);
  }

  public void setViewMaxEndpoints(int maxViewEndpoints) {
    this.couchbaseEnvBuilder.serviceConfig().viewServiceConfig().maxEndpoints(maxViewEndpoints);
  }

  public void setQueryEndpoints(int queryEndpoints) {
    this.couchbaseEnvBuilder.serviceConfig().queryServiceConfig().minEndpoints(queryEndpoints);
  }
   */
}
