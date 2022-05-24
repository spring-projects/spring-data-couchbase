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

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.CallbackPreferringPlatformTransactionManager;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

// todo gp experimenting with simplest possible CallbackPreferringPlatformTransactionManager, extending PlatformTransactionManager
// not AbstractPlatformTransactionManager
public class CouchbaseSimpleCallbackTransactionManager implements CallbackPreferringPlatformTransactionManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSimpleCallbackTransactionManager.class);

	private final CouchbaseClientFactory couchbaseClientFactory;

	public CouchbaseSimpleCallbackTransactionManager(CouchbaseClientFactory couchbaseClientFactory) {
		this.couchbaseClientFactory = couchbaseClientFactory;
	}

	@Override
	public <T> T execute(TransactionDefinition definition, TransactionCallback<T> callback) throws TransactionException {
		final AtomicReference<T> execResult = new AtomicReference<>();

			couchbaseClientFactory.getCluster().transactions().run(ctx -> {
				CouchbaseTransactionStatus status = new CouchbaseTransactionStatus(null, true, false, false, true, null, null);

				populateTransactionSynchronizationManager(ctx);

				try {
					execResult.set(callback.doInTransaction(status));
				}
				finally {
					TransactionSynchronizationManager.clear();
				}
			});

			TransactionSynchronizationManager.clear();

			return execResult.get();
	}

	private void populateTransactionSynchronizationManager(TransactionAttemptContext ctx) {
		// Setting ThreadLocal storage
		TransactionSynchronizationManager.setActualTransactionActive(true);
		TransactionSynchronizationManager.initSynchronization();
		// Oddly, TransactionSynchronizationManager.clear() does not clear resources
		try {
			TransactionSynchronizationManager.unbindResource(CoreTransactionAttemptContext.class);
		}
		// todo gp must be a nicer way...
		catch (IllegalStateException err) {}
		// todo gpx if we need this of course needs to be exposed nicely - apparently unsupported in JDK17 anyway
		CoreTransactionAttemptContext internal;
		try {
			Field field = TransactionAttemptContext.class.getDeclaredField("internal");
			field.setAccessible(true);
			internal = (CoreTransactionAttemptContext) field.get(ctx);
		}
		catch (Throwable err) {
			throw new RuntimeException(err);
		}
		TransactionSynchronizationManager.bindResource(CoreTransactionAttemptContext.class, internal);
	}

	@Override
	public TransactionStatus getTransaction(TransactionDefinition definition) throws TransactionException {
		throw new IllegalStateException("unimplemented!");
	}

	@Override
	public void commit(TransactionStatus status) throws TransactionException {
		// todo gpx somewhat nervous that commit/rollback/getTransaction are all left empty but things seem to be working
		// anyway... - what are these used for exactly?
		throw new IllegalStateException("unimplemented!");
	}

	@Override
	public void rollback(TransactionStatus status) throws TransactionException {
		throw new IllegalStateException("unimplemented!");
	}
}
