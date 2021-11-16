/*
 * Copyright 2021 the original author or authors
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
package org.springframework.data.couchbase.transaction;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.repository.DynamicProxyable;
import org.springframework.transaction.reactive.TransactionalOperator;

import com.couchbase.client.core.error.CouchbaseException;
// import com.couchbase.transactions.AttemptContextReactive;


/**
 * This is a proxy for AttemptContextReactive that also has the transactionalOperator, so that it can provide the
 * transactionalOperator to the repository and templates used within the transaction lambda via ctx.template(templ) and
 * ctx.repository(repo)
 */
public interface CouchbaseAttemptContextReactive {

	<R extends DynamicProxyable<R>> R repository(R repo);

	ReactiveCouchbaseTemplate template(ReactiveCouchbaseTemplate template);

	static CouchbaseAttemptContextReactive proxyFor(/*AttemptContextReactive acr,*/ TransactionalOperator txOperator) {
		Class<?>[] interfaces = new Class<?>[] { /* AttemptContextReactiveInterface.class, */
				CouchbaseAttemptContextReactive.class };
		CouchbaseAttemptContextReactive proxyInstance = (CouchbaseAttemptContextReactive) Proxy.newProxyInstance(
				txOperator.getClass().getClassLoader(), interfaces,
				new CouchbaseAttemptContextReactive.ACRInvocationHandler(/*acr,*/ txOperator));
		return proxyInstance;
	}

	class ACRInvocationHandler implements InvocationHandler {

	//	final AttemptContextReactive acr;
		final TransactionalOperator txOperator;

		public ACRInvocationHandler(/*AttemptContextReactive acr,*/ TransactionalOperator txOperator) {
//			this.acr = acr;
			this.txOperator = txOperator;
		}

		public ReactiveCouchbaseTemplate template(ReactiveCouchbaseTemplate template) {
			ReactiveCouchbaseTransactionManager txMgr = ((ReactiveCouchbaseTransactionManager) ((CouchbaseStuffHandle) txOperator)
					.getTransactionManager());
			if (template.getCouchbaseClientFactory() != txMgr.getDatabaseFactory()) {
				throw new CouchbaseException(
						"Template must use the same clientFactory as the transactionManager of the transactionalOperator "
								+ template);
			}
			return template;//.with((CouchbaseStuffHandle) txOperator); // this returns a new template with a new
																																					// couchbaseClient with txOperator
		}

		public <R extends DynamicProxyable<R>> R repository(R repo) {
			if (!(repo.getOperations() instanceof ReactiveCouchbaseOperations)) {
				throw new CouchbaseException("Repository must be a Reactive Couchbase repository" + repo);
			}
			ReactiveCouchbaseOperations reactiveOperations = (ReactiveCouchbaseOperations) repo.getOperations();
			ReactiveCouchbaseTransactionManager txMgr = ((ReactiveCouchbaseTransactionManager) ((CouchbaseStuffHandle) txOperator)
					.getTransactionManager());

			if (reactiveOperations.getCouchbaseClientFactory() != txMgr.getDatabaseFactory()) {
				throw new CouchbaseException(
						"Repository must use the same clientFactory as the transactionManager of the transactionalOperator "
								+ repo);
			}
			return repo.withTransaction((CouchbaseStuffHandle) txOperator); // this returns a new repository proxy with txOperator in its threadLocal
			// what if instead we returned a new repo with a new template with the txOperator?
		}

		@Override
		public Object invoke(Object o, Method method, Object[] objects) throws Throwable {
			if (method.getName().equals("template")) {
				return template((ReactiveCouchbaseTemplate) objects[0]);
			}
			if (method.getName().equals("repository")) {
				return repository((DynamicProxyable) objects[0]);
			}
			throw new UnsupportedOperationException(method.toString());
			//return method.invoke(acr, objects);
		}
	}

}
