/*
 * Copyright 2012-2020 the original author or authors
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
package org.springframework.data.couchbase.repository.support;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.core.NamedThreadLocal;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.RepositoryProxyPostProcessor;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import com.couchbase.client.core.io.CollectionIdentifier;

/**
 * {@link RepositoryProxyPostProcessor} that sets up interceptors to read metadata information from the invoked method.
 * This is necessary to allow redeclaration of CRUD methods in repository interfaces and configure locking information
 * or query hints on them.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Jens Schauder
 */
class CrudMethodMetadataPostProcessor implements RepositoryProxyPostProcessor, BeanClassLoaderAware {

	private @Nullable ClassLoader classLoader = ClassUtils.getDefaultClassLoader();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.BeanClassLoaderAware#setBeanClassLoader(java.lang.ClassLoader)
	 */
	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryProxyPostProcessor#postProcess(org.springframework.aop.framework.ProxyFactory, org.springframework.data.repository.core.RepositoryInformation)
	 */
	@Override
	public void postProcess(ProxyFactory factory, RepositoryInformation repositoryInformation) {
		factory.addAdvice(new CrudMethodMetadataPopulatingMethodInterceptor(repositoryInformation));
	}

	/**
	 * Returns a {@link CrudMethodMetadata} proxy that will lookup the actual target object by obtaining a thread bound
	 * instance from the {@link TransactionSynchronizationManager} later.
	 */
	CrudMethodMetadata getCrudMethodMetadata() {
		ProxyFactory factory = new ProxyFactory();
		factory.addInterface(CrudMethodMetadata.class);
		factory.setTargetSource(new ThreadBoundTargetSource());
		return (CrudMethodMetadata) factory.getProxy(this.classLoader);
	}

	/**
	 * {@link MethodInterceptor} to build and cache {@link DefaultCrudMethodMetadata} instances for the invoked methods.
	 * Will bind the found information to a {@link TransactionSynchronizationManager} for later lookup.
	 *
	 * @see DefaultCrudMethodMetadata
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */
	static class CrudMethodMetadataPopulatingMethodInterceptor implements MethodInterceptor {

		private static final ThreadLocal<MethodInvocation> currentInvocation = new NamedThreadLocal<>(
				"Current AOP method invocation");

		private final ConcurrentMap<Method, CrudMethodMetadata> metadataCache = new ConcurrentHashMap<>();
		private final Set<Method> implementations = new HashSet<>();

		CrudMethodMetadataPopulatingMethodInterceptor(RepositoryInformation repositoryInformation) {
			ReflectionUtils.doWithMethods(repositoryInformation.getRepositoryInterface(), implementations::add,
					method -> !repositoryInformation.isQueryMethod(method));
		}

		/**
		 * Return the AOP Alliance {@link MethodInvocation} object associated with the current invocation.
		 *
		 * @return the invocation object associated with the current invocation.
		 * @throws IllegalStateException if there is no AOP invocation in progress, or if the
		 *           {@link CrudMethodMetadataPopulatingMethodInterceptor} was not added to this interceptor chain.
		 */
		static MethodInvocation currentInvocation() throws IllegalStateException {

			MethodInvocation mi = currentInvocation.get();

			if (mi == null)
				throw new IllegalStateException(
						"No MethodInvocation found: Check that an AOP invocation is in progress, and that the "
								+ "CrudMethodMetadataPopulatingMethodInterceptor is upfront in the interceptor chain.");
			return mi;
		}

		/*
		 * (non-Javadoc)
		 * @see org.aopalliance.intercept.MethodInterceptor#invoke(org.aopalliance.intercept.MethodInvocation)
		 */
		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {

			Method method = invocation.getMethod();

			if (!implementations.contains(method)) {
				return invocation.proceed();
			}

			MethodInvocation oldInvocation = currentInvocation.get();
			currentInvocation.set(invocation);

			try {

				CrudMethodMetadata metadata = (CrudMethodMetadata) TransactionSynchronizationManager.getResource(method);

				if (metadata != null) {
					return invocation.proceed();
				}

				CrudMethodMetadata methodMetadata = metadataCache.get(method);

				if (methodMetadata == null) {

					methodMetadata = new DefaultCrudMethodMetadata(method);
					CrudMethodMetadata tmp = metadataCache.putIfAbsent(method, methodMetadata);

					if (tmp != null) {
						methodMetadata = tmp;
					}
				}

				TransactionSynchronizationManager.bindResource(method, methodMetadata);

				try {
					return invocation.proceed();
				} finally {
					// TransactionSynchronizationManager.unbindResource(method);
				}
			} finally {
				// currentInvocation.set(oldInvocation);
			}
		}
	}

	/**
	 * Default implementation of {@link CrudMethodMetadata} that will inspect the backing method for annotations.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */
	private static class DefaultCrudMethodMetadata implements CrudMethodMetadata {

		private final Method method;
		private final ScanConsistency scanConsistency;
		private String scope;
		private String collection;
		private Class<?> repositoryInterface;

		/**
		 * Creates a new {@link DefaultCrudMethodMetadata} for the given {@link Method}.
		 *
		 * @param method must not be {@literal null}.
		 */
		DefaultCrudMethodMetadata(Method method) {
			Assert.notNull(method, "Method must not be null!");
			this.method = method;

			ScanConsistency scanConsistency = null;
			String scope = CollectionIdentifier.DEFAULT_SCOPE;
			String collection = CollectionIdentifier.DEFAULT_COLLECTION;
			for (Annotation ann : method.getDeclaringClass().getAnnotations()) {
				if (ann instanceof ScanConsistency) {
					scanConsistency = ((ScanConsistency) ann);
				} else if (ann instanceof Document) {
					scope = ((Document) ann).scope();
					collection = ((Document) ann).collection();
				}
			}
			for (Annotation ann : method.getAnnotations()) {
				if (ann instanceof ScanConsistency) {
					scanConsistency = ((ScanConsistency) ann);
				}
			}

			this.scanConsistency = scanConsistency;
			this.scope = scope;
			this.collection = collection;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.jpa.repository.support.CrudMethodMetadata#getMethod()
		 */
		@Override
		public Method getMethod() {
			return method;
		}

		@Override
		public ScanConsistency getScanConsistency() {
			return scanConsistency;
		}

		@Override
		public String getScope() {
			return scope;
		}

		@Override
		public String getCollection() {
			return collection;
		}

		@Override
		public Class<?> repositoryInterface() {
			return repositoryInterface;
		}
	}

	private static class ThreadBoundTargetSource implements TargetSource {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.aop.TargetSource#getTargetClass()
		 */
		@Override
		public Class<?> getTargetClass() {
			return CrudMethodMetadata.class;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.aop.TargetSource#isStatic()
		 */
		@Override
		public boolean isStatic() {
			return false;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.aop.TargetSource#getTarget()
		 */
		@Override
		public Object getTarget() {

			MethodInvocation invocation = CrudMethodMetadataPopulatingMethodInterceptor.currentInvocation();
			return TransactionSynchronizationManager.getResource(invocation.getMethod());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.aop.TargetSource#releaseTarget(java.lang.Object)
		 */
		@Override
		public void releaseTarget(Object target) {}
	}
}
