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

import java.security.KeyStore;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.env.WaitStrategyFactory;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.metrics.MetricsCollector;
import com.couchbase.client.core.metrics.NetworkLatencyMetricsCollector;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import rx.Observable;
import rx.Scheduler;

/**
 * A proxy around a {@link CouchbaseEnvironment} that prevents its {@link #shutdown()} method
 * to be invoked. Useful when the delegate is not to be lifecycle-managed by Spring.
 *
 * @author Simon Baslé
 * @author Jonathan Edwards
 * @author Subhashni Balakrishnan
 */
public class CouchbaseEnvironmentNoShutdownProxy implements CouchbaseEnvironment {

  private final CouchbaseEnvironment delegate;

  public CouchbaseEnvironmentNoShutdownProxy(CouchbaseEnvironment delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean shutdown() {
    return false;
  }

  //===== DELEGATION METHODS =====

  @Override
  public Observable<Boolean> shutdownAsync() {
    return delegate.shutdownAsync();
  }

  @Override
  public EventLoopGroup ioPool() {
    return delegate.ioPool();
  }

  @Override
  public EventLoopGroup kvIoPool() {
    return delegate.kvIoPool();
  }

  @Override
  public Scheduler scheduler() {
    return delegate.scheduler();
  }

  @Override
  public boolean dcpEnabled() {
    return delegate.dcpEnabled();
  }

  @Override
  public boolean sslEnabled() {
    return delegate.sslEnabled();
  }

  @Override
  public String sslKeystoreFile() {
    return delegate.sslKeystoreFile();
  }

  @Override
  public String sslKeystorePassword() {
    return delegate.sslKeystorePassword();
  }

  @Override
  public boolean bootstrapHttpEnabled() {
    return delegate.bootstrapHttpEnabled();
  }

  @Override
  public boolean bootstrapCarrierEnabled() {
    return delegate.bootstrapCarrierEnabled();
  }

  @Override
  public int bootstrapHttpDirectPort() {
    return delegate.bootstrapHttpDirectPort();
  }

  @Override
  public int bootstrapHttpSslPort() {
    return delegate.bootstrapHttpSslPort();
  }

  @Override
  public int bootstrapCarrierDirectPort() {
    return delegate.bootstrapCarrierDirectPort();
  }

  @Override
  public int bootstrapCarrierSslPort() {
    return delegate.bootstrapCarrierSslPort();
  }

  @Override
  public int ioPoolSize() {
    return delegate.ioPoolSize();
  }

  @Override
  public int computationPoolSize() {
    return delegate.computationPoolSize();
  }

  @Override
  public Delay observeIntervalDelay() {
    return delegate.observeIntervalDelay();
  }

  @Override
  public Delay reconnectDelay() {
    return delegate.reconnectDelay();
  }

  @Override
  public Delay retryDelay() {
    return delegate.retryDelay();
  }

  @Override
  public int requestBufferSize() {
    return delegate.requestBufferSize();
  }

  @Override
  public int responseBufferSize() {
    return delegate.responseBufferSize();
  }

  @Override
  public int dcpConnectionBufferSize() {
    return delegate.dcpConnectionBufferSize();
  }

  @Override
  public double dcpConnectionBufferAckThreshold() {
    return delegate.dcpConnectionBufferAckThreshold();
  }

  @Override
  public int kvEndpoints() {
    return delegate.kvEndpoints();
  }

  @Override
  public int viewEndpoints() {
    return delegate.viewEndpoints();
  }

  @Override
  public int queryEndpoints() {
    return delegate.queryEndpoints();
  }

  @Override
  public String userAgent() {
    return delegate.userAgent();
  }

  @Override
  public String packageNameAndVersion() {
    return delegate.packageNameAndVersion();
  }

  @Override
  public RetryStrategy retryStrategy() {
    return delegate.retryStrategy();
  }

  @Override
  public long maxRequestLifetime() {
    return delegate.maxRequestLifetime();
  }

  @Override
  public long autoreleaseAfter() {
    return delegate.autoreleaseAfter();
  }

  @Override
  public long keepAliveInterval() {
    return delegate.keepAliveInterval();
  }

  @Override
  public EventBus eventBus() {
    return delegate.eventBus();
  }

  @Override
  public boolean bufferPoolingEnabled() {
    return delegate.bufferPoolingEnabled();
  }

  @Override
  public long managementTimeout() {
    return delegate.managementTimeout();
  }

  @Override
  public long queryTimeout() {
    return delegate.queryTimeout();
  }

  @Override
  public long viewTimeout() {
    return delegate.viewTimeout();
  }

  @Override
  public long kvTimeout() {
    return delegate.kvTimeout();
  }

  @Override
  public long connectTimeout() {
    return delegate.connectTimeout();
  }

  @Override
  public long disconnectTimeout() {
    return delegate.disconnectTimeout();
  }

  @Override
  public boolean dnsSrvEnabled() {
    return delegate.dnsSrvEnabled();
  }

  @Override
  public NetworkLatencyMetricsCollector networkLatencyMetricsCollector() {
    return delegate.networkLatencyMetricsCollector();
  }

  @Override
  public int socketConnectTimeout() {
    return delegate.socketConnectTimeout();
  }

  @Override
  public MetricsCollector runtimeMetricsCollector() {
    return delegate.runtimeMetricsCollector();
  }

  @Override
  public boolean mutationTokensEnabled() {
    return delegate.mutationTokensEnabled();
  }

  @Override
  public boolean tcpNodelayEnabled() {
    return delegate.tcpNodelayEnabled();
  }

  @Override
  public boolean callbacksOnIoPool() {
    return delegate.callbacksOnIoPool();
  }

  @Override
  public String coreBuild() {
    return delegate.coreBuild();
  }

  @Override
  public String coreVersion() {
    return delegate.coreVersion();
  }

  @Override
  public String dcpConnectionName() {
    return delegate.dcpConnectionName();
  }

  @Override
  public int searchEndpoints() {
    return delegate.searchEndpoints();
  }

  @Override
  public String clientBuild() {
    return delegate.clientBuild();
  }

  @Override
  public String clientVersion() {
    return delegate.clientVersion();
  }

  @Override
  public long searchTimeout() {
    return delegate.searchTimeout();
  }

  @Override
  public WaitStrategyFactory requestBufferWaitStrategy() {
    return delegate.requestBufferWaitStrategy();
  }

  @Override
  public EventLoopGroup viewIoPool() {
    return delegate.viewIoPool();
  }

  @Override
  public EventLoopGroup searchIoPool() {
    return delegate.searchIoPool();
  }

  @Override
  public EventLoopGroup queryIoPool() {
    return delegate.queryIoPool();
  }

  @Override
  public KeyStore sslKeystore() {
    return delegate.sslKeystore();
  }

  @Override
  public boolean shutdown(long timeout, TimeUnit timeUnit) {
    return delegate.shutdown(timeout, timeUnit);
  }

  @Override
  public MemcachedHashingStrategy memcachedHashingStrategy() {
    return delegate.memcachedHashingStrategy();
  }
}
