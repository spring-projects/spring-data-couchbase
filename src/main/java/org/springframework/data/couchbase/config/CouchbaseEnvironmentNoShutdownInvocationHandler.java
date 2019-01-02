/*
 * Copyright 2012-2019 the original author or authors
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
import com.couchbase.client.java.env.CouchbaseEnvironment;

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
    if (method.getName().contentEquals("shutdown")) {
      return false;
    }
    return method.invoke(this.environment, args);
  }
}
