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
package org.springframework.data.couchbase.transactions;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.CallbackPreferringPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceHolderSupport;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronizationUtils;
import org.springframework.util.Assert;

import com.couchbase.transactions.AttemptContextReactive;
import com.couchbase.transactions.AttemptContextReactiveAccessor;
import com.couchbase.transactions.TransactionResult;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.config.TransactionConfig;

/**
 * Blocking TransactionManager
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */

public class CouchbaseTransactionManager extends AbstractPlatformTransactionManager
		implements DisposableBean, ResourceTransactionManager, CallbackPreferringPlatformTransactionManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTransactionManager.class);

	private final CouchbaseTemplate template;
	private final Transactions transactions;

	public CouchbaseTransactionManager(CouchbaseTemplate template, TransactionConfig transactionConfig) {
		this.template = template;
		this.transactions = Transactions.create(template.getCouchbaseClientFactory().getCluster(), transactionConfig);
	}

	public CouchbaseTemplate template() {
		return template;
	}

	@Override
	public <T> T execute(TransactionDefinition definition, TransactionCallback<T> callback) throws TransactionException {
		final AtomicReference<T> result = new AtomicReference<>();
		TransactionResult txnResult = transactions.run(attemptContext -> {

			if (TransactionSynchronizationManager.hasResource(template.getCouchbaseClientFactory())) {
				((CouchbaseResourceHolder) TransactionSynchronizationManager
						.getResource(template.reactive().getCouchbaseClientFactory()))
								.setAttemptContext(AttemptContextReactiveAccessor.getACR(attemptContext));
			} else {
				TransactionSynchronizationManager.bindResource(template.reactive().getCouchbaseClientFactory(),
						new CouchbaseResourceHolder(AttemptContextReactiveAccessor.getACR(attemptContext)));
			}

			try {
				// Since we are on a different thread now transparently, at least make sure
				// that the original method invocation is synchronized.
				synchronized (this) {
					result.set(callback.doInTransaction(null));
				}
			} catch (RuntimeException e) {
				throw e;
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		});

		LOGGER.debug("Completed Couchbase Transaction with Result: " + txnResult);
		return result.get();
	}

	@Override
	protected CouchbaseTransactionObject doGetTransaction() throws TransactionException {
		CouchbaseResourceHolder resourceHolder = (CouchbaseResourceHolder) TransactionSynchronizationManager
				.getResource(template.getCouchbaseClientFactory());
		return new CouchbaseTransactionObject(resourceHolder);
	}

	@Override
	protected boolean isExistingTransaction(Object transaction) throws TransactionException {
		return extractTransaction(transaction).hasResourceHolder();
	}

	@Override
	protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		LOGGER.debug("Beginning Couchbase Transaction with Definition {}", definition);
	}

	@Override
	protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
		LOGGER.debug("Committing Couchbase Transaction with status {}", status);
	}

	@Override
	protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		LOGGER.warn("Rolling back Couchbase Transaction with status {}", status);
	}

	@Override
	protected void doCleanupAfterCompletion(Object transaction) {
		LOGGER.trace("Performing cleanup of Couchbase Transaction {}", transaction);
	}

	@Override
	public void destroy() {
		transactions.close();
	}

	@Override
	public Object getResourceFactory() {
		return template.getCouchbaseClientFactory();
	}

	private static CouchbaseTransactionObject extractTransaction(Object transaction) {
		Assert.isInstanceOf(CouchbaseTransactionObject.class, transaction,
				() -> String.format("Expected to find a %s but it turned out to be %s.", CouchbaseTransactionObject.class,
						transaction.getClass()));

		return (CouchbaseTransactionObject) transaction;
	}

	public class CouchbaseResourceHolder extends ResourceHolderSupport {

		private volatile AttemptContextReactive attemptContext;
		private volatile TransactionResultMap resultMap = new TransactionResultMap(template);

		public CouchbaseResourceHolder(AttemptContextReactive attemptContext) {
			this.attemptContext = attemptContext;
		}

		public AttemptContextReactive getAttemptContext() {
			return attemptContext;
		}

		public void setAttemptContext(AttemptContextReactive attemptContext) {
			this.attemptContext = attemptContext;
		}

		public TransactionResultMap getTxResultMap() {
			return resultMap;
		}

		@Override
		public String toString() {
			return "CouchbaseResourceHolder{" + "attemptContext=" + attemptContext + " txResultMap=" + resultMap + '}';
		}
	}

	protected static class CouchbaseTransactionObject implements SmartTransactionObject {

		private final CouchbaseResourceHolder resourceHolder;

		CouchbaseTransactionObject(CouchbaseResourceHolder resourceHolder) {
			this.resourceHolder = resourceHolder;
		}

		@Override
		public boolean isRollbackOnly() {
			return this.resourceHolder != null && this.resourceHolder.isRollbackOnly();
		}

		@Override
		public void flush() {
			TransactionSynchronizationUtils.triggerFlush();
		}

		public boolean hasResourceHolder() {
			return resourceHolder != null;
		}

		@Override
		public String toString() {
			return "CouchbaseTransactionObject{" + "resourceHolder=" + resourceHolder + '}';
		}
	}

}
