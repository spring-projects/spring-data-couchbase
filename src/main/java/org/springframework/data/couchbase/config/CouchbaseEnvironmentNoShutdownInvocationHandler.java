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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.env.*;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.metrics.MetricsCollector;
import com.couchbase.client.core.metrics.NetworkLatencyMetricsCollector;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import rx.Observable;
import rx.Scheduler;

/**
 * A dynamic proxy around a {@link CouchbaseEnvironment} that prevents its {@link CouchbaseEnvironment#shutdown()} method
 * to be invoked. Useful when the delegate is not to be lifecycle-managed by Spring.
 *
 * @author Simon Baslé
 * @author Jonathan Edwards
 * @author Subhashni Balakrishnan
 */
public class CouchbaseEnvironmentNoShutdownInvocationHandler implements InvocationHandler {

  private final CouchbaseEnvironment environment;

  public CouchbaseEnvironmentNoShutdownInvocationHandler(CouchbaseEnvironment environment) {
    this.environment = environment;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if (method.getName().compareTo("shutdown") == 0) {
      return false;
    }
    return method.invoke(this.environment, args);
  }
}