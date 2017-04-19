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

import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import org.springframework.beans.factory.config.AbstractFactoryBean;

/**
 * Factory Bean to help create a CouchbaseEnvironment (by offering setters for supported tuning methods).
 *
 * @author Simon Baslé
 */
/*package*/ class CouchbaseEnvironmentFactoryBean extends AbstractFactoryBean<CouchbaseEnvironment> {

  private static final CouchbaseEnvironment DEFAULT_ENV = DefaultCouchbaseEnvironment.create();
  public static final String RETRYSTRATEGY_FAILFAST = "FailFast";
  public static final String RETRYSTRATEGY_BESTEFFORT = "BestEffort";

  private long managementTimeout = DEFAULT_ENV.managementTimeout();
  private long queryTimeout = DEFAULT_ENV.queryTimeout();
  private long viewTimeout = DEFAULT_ENV.viewTimeout();
  private long kvTimeout = DEFAULT_ENV.kvTimeout();
  private long connectTimeout = DEFAULT_ENV.connectTimeout();
  private long disconnectTimeout = DEFAULT_ENV.disconnectTimeout();
  private boolean dnsSrvEnabled = DEFAULT_ENV.dnsSrvEnabled();

  private boolean dcpEnabled = DEFAULT_ENV.dcpEnabled();
  private boolean sslEnabled = DEFAULT_ENV.sslEnabled();
  private String sslKeystoreFile = DEFAULT_ENV.sslKeystoreFile();
  private String sslKeystorePassword = DEFAULT_ENV.sslKeystorePassword();
  private boolean queryEnabled = DEFAULT_ENV.queryEnabled();
  private int queryPort = DEFAULT_ENV.queryPort();
  private boolean bootstrapHttpEnabled = DEFAULT_ENV.bootstrapHttpEnabled();
  private boolean bootstrapCarrierEnabled = DEFAULT_ENV.bootstrapCarrierEnabled();
  private int bootstrapHttpDirectPort = DEFAULT_ENV.bootstrapHttpDirectPort();
  private int bootstrapHttpSslPort = DEFAULT_ENV.bootstrapHttpSslPort();
  private int bootstrapCarrierDirectPort = DEFAULT_ENV.bootstrapCarrierDirectPort();
  private int bootstrapCarrierSslPort = DEFAULT_ENV.bootstrapCarrierSslPort();
  private int ioPoolSize = DEFAULT_ENV.ioPoolSize();
  private int computationPoolSize = DEFAULT_ENV.computationPoolSize();
  private int responseBufferSize = DEFAULT_ENV.responseBufferSize();
  private int requestBufferSize = DEFAULT_ENV.requestBufferSize();
  private int kvEndpoints = DEFAULT_ENV.kvEndpoints();
  private int viewEndpoints = DEFAULT_ENV.viewEndpoints();
  private int queryEndpoints = DEFAULT_ENV.queryEndpoints();
  private RetryStrategy retryStrategy = DEFAULT_ENV.retryStrategy();
  private long maxRequestLifetime = DEFAULT_ENV.maxRequestLifetime();
  private long keepAliveInterval = DEFAULT_ENV.keepAliveInterval();
  private long autoreleaseAfter = DEFAULT_ENV.autoreleaseAfter();
  private boolean bufferPoolingEnabled = DEFAULT_ENV.bufferPoolingEnabled();
  private boolean tcpNodelayEnabled = DEFAULT_ENV.tcpNodelayEnabled();
  private boolean mutationTokensEnabled = DEFAULT_ENV.mutationTokensEnabled();

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
    return DefaultCouchbaseEnvironment.builder()
        .managementTimeout(managementTimeout)
        .queryTimeout(queryTimeout)
        .viewTimeout(viewTimeout)
        .kvTimeout(kvTimeout)
        .connectTimeout(connectTimeout)
        .disconnectTimeout(disconnectTimeout)
        .dnsSrvEnabled(dnsSrvEnabled)
        .dcpEnabled(dcpEnabled)
        .sslEnabled(sslEnabled)
        .sslKeystoreFile(sslKeystoreFile)
        .sslKeystorePassword(sslKeystorePassword)
        .queryEnabled(queryEnabled)
        .queryPort(queryPort)
        .bootstrapHttpEnabled(bootstrapHttpEnabled)
        .bootstrapCarrierEnabled(bootstrapCarrierEnabled)
        .bootstrapHttpDirectPort(bootstrapHttpDirectPort)
        .bootstrapHttpSslPort(bootstrapHttpSslPort)
        .bootstrapCarrierDirectPort(bootstrapCarrierDirectPort)
        .bootstrapCarrierSslPort(bootstrapCarrierSslPort)
        .ioPoolSize(ioPoolSize)
        .computationPoolSize(computationPoolSize)
        .responseBufferSize(responseBufferSize)
        .requestBufferSize(requestBufferSize)
        .kvEndpoints(kvEndpoints)
        .viewEndpoints(viewEndpoints)
        .queryEndpoints(queryEndpoints)
        .retryStrategy(retryStrategy)
        .maxRequestLifetime(maxRequestLifetime)
        .keepAliveInterval(keepAliveInterval)
        .autoreleaseAfter(autoreleaseAfter)
        .bufferPoolingEnabled(bufferPoolingEnabled)
        .tcpNodelayEnabled(tcpNodelayEnabled)
        .mutationTokensEnabled(mutationTokensEnabled)
        .build();
  }

  /**
   * Sets the {@link RetryStrategy} to use from an enum-like String value.
   * Either "FailFast" or "BestEffort" are recognized.
   *
   * @param retryStrategy the string value enum from which to choose a strategy.
   */
  public void setRetryStrategy(String retryStrategy) {
    if (RETRYSTRATEGY_FAILFAST.equals(retryStrategy)) {
      this.retryStrategy = FailFastRetryStrategy.INSTANCE;
    } else if (RETRYSTRATEGY_BESTEFFORT.equals(retryStrategy)) {
      this.retryStrategy = BestEffortRetryStrategy.INSTANCE;
    }
  }

  //==== SETTERS for the factory bean ====

  public void setManagementTimeout(long managementTimeout) {
    this.managementTimeout = managementTimeout;
  }

  public void setQueryTimeout(long queryTimeout) {
    this.queryTimeout = queryTimeout;
  }

  public void setViewTimeout(long viewTimeout) {
    this.viewTimeout = viewTimeout;
  }

  public void setKvTimeout(long kvTimeout) {
    this.kvTimeout = kvTimeout;
  }

  public void setConnectTimeout(long connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  public void setDisconnectTimeout(long disconnectTimeout) {
    this.disconnectTimeout = disconnectTimeout;
  }

  public void setDnsSrvEnabled(boolean dnsSrvEnabled) {
    this.dnsSrvEnabled = dnsSrvEnabled;
  }

  public void setDcpEnabled(boolean dcpEnabled) {
    this.dcpEnabled = dcpEnabled;
  }

  public void setSslEnabled(boolean sslEnabled) {
    this.sslEnabled = sslEnabled;
  }

  public void setSslKeystoreFile(String sslKeystoreFile) {
    this.sslKeystoreFile = sslKeystoreFile;
  }

  public void setSslKeystorePassword(String sslKeystorePassword) {
    this.sslKeystorePassword = sslKeystorePassword;
  }

  public void setQueryEnabled(boolean queryEnabled) {
    this.queryEnabled = queryEnabled;
  }

  public void setQueryPort(int queryPort) {
    this.queryPort = queryPort;
  }

  public void setBootstrapHttpEnabled(boolean bootstrapHttpEnabled) {
    this.bootstrapHttpEnabled = bootstrapHttpEnabled;
  }

  public void setBootstrapCarrierEnabled(boolean bootstrapCarrierEnabled) {
    this.bootstrapCarrierEnabled = bootstrapCarrierEnabled;
  }

  public void setBootstrapHttpDirectPort(int bootstrapHttpDirectPort) {
    this.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
  }

  public void setBootstrapHttpSslPort(int bootstrapHttpSslPort) {
    this.bootstrapHttpSslPort = bootstrapHttpSslPort;
  }

  public void setBootstrapCarrierDirectPort(int bootstrapCarrierDirectPort) {
    this.bootstrapCarrierDirectPort = bootstrapCarrierDirectPort;
  }

  public void setBootstrapCarrierSslPort(int bootstrapCarrierSslPort) {
    this.bootstrapCarrierSslPort = bootstrapCarrierSslPort;
  }

  public void setIoPoolSize(int ioPoolSize) {
    this.ioPoolSize = ioPoolSize;
  }

  public void setComputationPoolSize(int computationPoolSize) {
    this.computationPoolSize = computationPoolSize;
  }

  public void setResponseBufferSize(int responseBufferSize) {
    this.responseBufferSize = responseBufferSize;
  }

  public void setRequestBufferSize(int requestBufferSize) {
    this.requestBufferSize = requestBufferSize;
  }

  public void setKvEndpoints(int kvEndpoints) {
    this.kvEndpoints = kvEndpoints;
  }

  public void setViewEndpoints(int viewEndpoints) {
    this.viewEndpoints = viewEndpoints;
  }

  public void setQueryEndpoints(int queryEndpoints) {
    this.queryEndpoints = queryEndpoints;
  }

  public void setMaxRequestLifetime(long maxRequestLifetime) {
    this.maxRequestLifetime = maxRequestLifetime;
  }

  public void setKeepAliveInterval(long keepAliveInterval) {
    this.keepAliveInterval = keepAliveInterval;
  }

  public void setAutoreleaseAfter(long autoreleaseAfter) {
    this.autoreleaseAfter = autoreleaseAfter;
  }

  public void setBufferPoolingEnabled(boolean bufferPoolingEnabled) {
    this.bufferPoolingEnabled = bufferPoolingEnabled;
  }

  public void setTcpNodelayEnabled(boolean tcpNodelayEnabled) {
    this.tcpNodelayEnabled = tcpNodelayEnabled;
  }

  public void setMutationTokensEnabled(boolean mutationTokensEnabled) {
    this.mutationTokensEnabled = mutationTokensEnabled;
  }
}
