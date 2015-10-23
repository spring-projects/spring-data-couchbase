/*
 * Copyright 2013-2014 the original author or authors.
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

package org.springframework.data.couchbase.repository.support;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.interceptor.ExposeInvocationInterceptor;
import org.springframework.core.NamedThreadLocal;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.RepositoryProxyPostProcessor;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link RepositoryProxyPostProcessor} that sets up an interceptor to read {@link View} information from the
 * invoked method. This is necessary to allow redeclaration of CRUD methods in repository interfaces and configure
 * view information on them.
 *
 * @author David Harrigan
 * @author Oliver Gierke
 */
public enum ViewPostProcessor implements RepositoryProxyPostProcessor {

  INSTANCE;

  private static final ThreadLocal<Map<Object, Object>> VIEW_METADATA = new NamedThreadLocal<Map<Object, Object>>("View Metadata");

  /* 
   * (non-Javadoc)
   * @see org.springframework.data.repository.core.support.RepositoryProxyPostProcessor#postProcess(org.springframework.aop.framework.ProxyFactory, org.springframework.data.repository.core.RepositoryInformation)
   */
  @Override
  public void postProcess(ProxyFactory factory, RepositoryInformation repositoryInformation) {
  	
    factory.addAdvice(ExposeInvocationInterceptor.INSTANCE);
    factory.addAdvice(ViewInterceptor.INSTANCE);
  }

  public ViewMetadataProvider getViewMetadataProvider() {
    return ThreadBoundViewMetadata.INSTANCE;
  }

  /**
   * {@link MethodInterceptor} to inspect the currently invoked {@link Method} for a {@link View} annotation.
   * <p/>
   * If a View annotation is found, it will bind it to a locally held ThreadLocal for later lookup in the
   * SimpleCouchbaseRepository class.
   *
   * @author David Harrigan.
   */
  static enum ViewInterceptor implements MethodInterceptor {

    INSTANCE;

    @Override
    public Object invoke(final MethodInvocation invocation) throws Throwable {

      final View view = AnnotationUtils.getAnnotation(invocation.getMethod(), View.class);
      if (view != null) {
        Map<Object, Object> map = VIEW_METADATA.get();
        if (map == null) {
          map = new HashMap<Object, Object>();
          VIEW_METADATA.set(map);
        }
        map.put(invocation.getMethod(), view);
      }
      try {
        return invocation.proceed();
      } finally {
        VIEW_METADATA.remove();
      }

    }

  }

  /**
   * {@link ViewMetadataProvider} that looks up a bound View from a locally held ThreadLocal, using
   * the current method invocationas as the key. If not bound View is found, a null is returned.
   *
   * @author David Harrigan.
   */
  private static enum ThreadBoundViewMetadata implements ViewMetadataProvider {

    INSTANCE;

    @Override
    public View getView() {
      final MethodInvocation invocation = ExposeInvocationInterceptor.currentInvocation();
      final Map<Object, Object> map = VIEW_METADATA.get();
      return (map == null) ? null : (View) map.get(invocation.getMethod());
    }

  }


}
