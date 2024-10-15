/*
 * Copyright 2022-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.transaction;

import java.io.Serializable;
import java.lang.reflect.Method;

import com.couchbase.client.core.annotation.Stability;
import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.lang.Nullable;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * This allows reactive @Transactional support with Couchbase transactions.
 * <p>
 * The ReactiveTransactionManager does not support the lambda-based nature of Couchbase transactions,
 * and there is no reactive equivalent of CallbackPreferringTransactionManager (which does).
 * <p>
 * The solution: override the standard TransactionInterceptor and, if the
 * CouchbaseCallbackTransactionManager is the provided TransactionManager, defer to that.
 *
 * @author Graham Pople
 * @author Michael Reiche
 */
@Stability.Internal
public class CouchbaseTransactionInterceptor extends TransactionInterceptor
		implements MethodInterceptor, Serializable {

	public CouchbaseTransactionInterceptor(TransactionManager ptm, TransactionAttributeSource tas) {
		super(ptm, tas);
	}

	@Nullable
	protected Object invokeWithinTransaction(Method method, @Nullable Class<?> targetClass,
			final InvocationCallback invocation) throws Throwable {
		final TransactionAttributeSource tas = getTransactionAttributeSource();
		final TransactionAttribute txAttr = (tas != null ? tas.getTransactionAttribute(method, targetClass) : null);

		if (getTransactionManager() instanceof CouchbaseCallbackTransactionManager) {
			CouchbaseCallbackTransactionManager manager = (CouchbaseCallbackTransactionManager) getTransactionManager();

			if (Mono.class.isAssignableFrom(method.getReturnType())) {
				return manager.executeReactive(txAttr, ignored -> {
					try {
						return (Mono<?>) invocation.proceedWithInvocation();
					} catch (RuntimeException e) {
						throw e;
					} catch (Throwable e) {
						throw new RuntimeException(e);
					}
				}).singleOrEmpty();
			} else if (Flux.class.isAssignableFrom(method.getReturnType())) {
				return manager.executeReactive(txAttr, ignored -> {
					try {
						return (Flux<?>) invocation.proceedWithInvocation();
					} catch (RuntimeException e) {
						throw e;
					} catch (Throwable e) {
						throw new RuntimeException(e);
					}
				});
			} else {
				return manager.execute(txAttr, ignored -> {
					try {
						return invocation.proceedWithInvocation();
					} catch (RuntimeException e) {
						throw e;
					} catch (Throwable e) {
						throw new RuntimeException(e);
					}
				});
			}
		} else {
			return super.invokeWithinTransaction(method, targetClass, invocation);
		}
	}
}
