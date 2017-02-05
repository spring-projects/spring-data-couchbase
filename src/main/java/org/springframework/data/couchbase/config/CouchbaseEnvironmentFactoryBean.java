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

import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment.Builder;

import org.springframework.beans.factory.config.AbstractFactoryBean;

/**
 * Factory Bean to help create a CouchbaseEnvironment (by offering setters for supported tuning methods).
 *
 * @author Simon Baslé
 * @author Simon Bland
 * @author Subhashni Balakrishnan
 */
/*package*/ class CouchbaseEnvironmentFactoryBean extends AbstractFactoryBean<CouchbaseEnvironment> {

  public static final String RETRYSTRATEGY_FAILFAST = "FailFast";
  public static final String RETRYSTRATEGY_BESTEFFORT = "BestEffort";
  
  private final Builder couchbaseEnvBuilder = DefaultCouchbaseEnvironment.builder();

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
   */

  @Override
  public Class<?> getObjectType() {
    return DefaultCouchbaseEnvironment.class;
  }

  @Override
  protected CouchbaseEnvironment createInstance() throws Exception {
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
    this.couchbaseEnvBuilder.managementTimeout(managementTimeout);
  }

  public void setQueryTimeout(long queryTimeout) {
    this.couchbaseEnvBuilder.queryTimeout(queryTimeout);
  }

  public void setViewTimeout(long viewTimeout) {
    this.couchbaseEnvBuilder.viewTimeout(viewTimeout);
  }

  public void setKvTimeout(long kvTimeout) {
    this.couchbaseEnvBuilder.kvTimeout(kvTimeout);
  }

  public void setConnectTimeout(long connectTimeout) {
    this.couchbaseEnvBuilder.connectTimeout(connectTimeout);
  }

  public void setDisconnectTimeout(long disconnectTimeout) {
    this.couchbaseEnvBuilder.disconnectTimeout(disconnectTimeout);
  }

  public void setDnsSrvEnabled(boolean dnsSrvEnabled) {
    this.couchbaseEnvBuilder.dnsSrvEnabled(dnsSrvEnabled);
  }

  public void setDcpEnabled(boolean dcpEnabled) {
    this.couchbaseEnvBuilder.dcpEnabled(dcpEnabled);
  }

  public void setSslEnabled(boolean sslEnabled) {
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
    this.couchbaseEnvBuilder.kvEndpoints(kvEndpoints);
  }

  public void setViewEndpoints(int viewEndpoints) {
    this.couchbaseEnvBuilder.viewEndpoints(viewEndpoints);
  }

  public void setQueryEndpoints(int queryEndpoints) {
    this.couchbaseEnvBuilder.queryEndpoints(queryEndpoints);
  }

  public void setMaxRequestLifetime(long maxRequestLifetime) {
    this.couchbaseEnvBuilder.maxRequestLifetime(maxRequestLifetime);
  }

  public void setKeepAliveInterval(long keepAliveInterval) {
    this.couchbaseEnvBuilder.keepAliveInterval(keepAliveInterval);
  }

  public void setAutoreleaseAfter(long autoreleaseAfter) {
    this.couchbaseEnvBuilder.autoreleaseAfter(autoreleaseAfter);
  }

  public void setBufferPoolingEnabled(boolean bufferPoolingEnabled) {
    this.couchbaseEnvBuilder.bufferPoolingEnabled(bufferPoolingEnabled);
  }

  public void setTcpNodelayEnabled(boolean tcpNodelayEnabled) {
    this.couchbaseEnvBuilder.tcpNodelayEnabled(tcpNodelayEnabled);
  }

  public void setMutationTokensEnabled(boolean mutationTokensEnabled) {
    this.couchbaseEnvBuilder.mutationTokensEnabled(mutationTokensEnabled);
  }
}
