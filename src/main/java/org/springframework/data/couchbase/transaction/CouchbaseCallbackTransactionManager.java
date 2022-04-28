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

import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.reactive.TransactionContextManager;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.CallbackPreferringPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionSynchronizationUtils;
import org.springframework.util.Assert;

/**
 * Blocking TransactionManager
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */

public class CouchbaseCallbackTransactionManager extends AbstractPlatformTransactionManager
		implements DisposableBean, ResourceTransactionManager, CallbackPreferringPlatformTransactionManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTransactionManager.class);

	private final CouchbaseTemplate template;
	private final ReactiveCouchbaseTemplate reactiveTemplate;
	private final ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory;
	private final CouchbaseClientFactory couchbaseClientFactory;

	private ReactiveCouchbaseTransactionManager.ReactiveCouchbaseTransactionObject transaction;

	public CouchbaseCallbackTransactionManager(CouchbaseTemplate template, ReactiveCouchbaseTemplate reactiveTemplate) {
		this.template = template;
		this.reactiveTemplate = reactiveTemplate;
		this.reactiveCouchbaseClientFactory = this.reactiveTemplate.getCouchbaseClientFactory();
		this.couchbaseClientFactory = this.template.getCouchbaseClientFactory();
	}

	public ReactiveCouchbaseTemplate template() {
		return reactiveTemplate;
	}

	private CouchbaseResourceHolder newResourceHolder(TransactionDefinition definition, ClientSessionOptions options,
			ReactiveTransactionAttemptContext atr) {

		CouchbaseClientFactory databaseFactory = template.getCouchbaseClientFactory();

		CouchbaseResourceHolder resourceHolder = new CouchbaseResourceHolder(
				databaseFactory.getSession(options, atr), databaseFactory);
		return resourceHolder;
	}

	@Override
	public <T> T execute(TransactionDefinition definition, TransactionCallback<T> callback) throws TransactionException {
		final AtomicReference<T> execResult = new AtomicReference<>();
		AtomicReference<Long> startTime = new AtomicReference<>(0L);

		Mono<TransactionResult> txnResult = couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> {
			/* begin spring-data-couchbase transaction 1/2 */
			ClientSession clientSession = reactiveCouchbaseClientFactory // couchbaseClientFactory
					.getSession(ClientSessionOptions.builder().causallyConsistent(true).build())
					.block();
			ReactiveCouchbaseResourceHolder reactiveResourceHolder = new ReactiveCouchbaseResourceHolder(clientSession,
					reactiveCouchbaseClientFactory);

			CouchbaseResourceHolder resourceHolder = new CouchbaseResourceHolder(clientSession,
					template.getCouchbaseClientFactory());

			Mono<TransactionSynchronizationManager> sync = TransactionContextManager.currentContext()
					.map(TransactionSynchronizationManager::new)
					.<TransactionSynchronizationManager> flatMap(synchronizationManager -> {
						System.err.println("CallbackTransactionManager: " + this);
						System.err.println("bindResource: " + reactiveCouchbaseClientFactory.getCluster().block());
						// todo gp not sure why we bind, unbind, bind again?
						synchronizationManager.bindResource(reactiveCouchbaseClientFactory.getCluster().block(),
								reactiveResourceHolder);
						org.springframework.transaction.support.TransactionSynchronizationManager
								.unbindResourceIfPossible(reactiveCouchbaseClientFactory.getCluster().block());
						org.springframework.transaction.support.TransactionSynchronizationManager
						 .bindResource(reactiveCouchbaseClientFactory.getCluster().block(), resourceHolder);
						ReactiveCouchbaseTransactionManager.ReactiveCouchbaseTransactionObject transaction = new ReactiveCouchbaseTransactionManager.ReactiveCouchbaseTransactionObject(
								reactiveResourceHolder);
						setTransaction(transaction);

						synchronizationManager.bindResource(ReactiveTransactionAttemptContext.class, ctx);

						/* end spring-data-couchbase transaction  1/2 */

						// todo gp do we need TransactionSynchronizationManager.forCurrentTransaction()? as we already have synchronizationManager
						Mono<Void> result = TransactionSynchronizationManager.forCurrentTransaction().flatMap((sm) -> {
							// todo gp not sure why re-binding again?
							sm.unbindResourceIfPossible(reactiveCouchbaseClientFactory.getCluster().block());
							sm.bindResource(reactiveCouchbaseClientFactory.getCluster().block(),
									reactiveResourceHolder);
							CouchbaseTransactionStatus status = new CouchbaseTransactionStatus(transaction, true, false, false, true, null, sm);
							prepareSynchronization(status, new CouchbaseTransactionDefinition());
							// System.err.println("deferContextual.ctx : " + xxx);
							//Mono<ContextView> cxView = Mono.deferContextual(cx -> { System.err.println("CallbackTransactionManager.cx: "+cx); return Mono.just(cx);});
							try {
								// Since we are on a different thread now transparently, at least make sure
								// that the original method invocation is synchronized.
								synchronized (this) {
									// todo gp this will execute the lambda, and so we likely don't want that to be inside a synchronized block
									execResult.set(callback.doInTransaction(status));
								}
							} catch (RuntimeException e) {
								throw e;
							} catch (Throwable e) {
								throw new RuntimeException(e);
							}
							return Mono.empty();
						}).contextWrite(TransactionContextManager.getOrCreateContext()) // this doesn't create a context on the desired publisher
								.contextWrite(TransactionContextManager.getOrCreateContextHolder()).then();

						// todo gp this isn't part of the chain (no `result = result.onErrorResume...`) so isn't called
						// and presumably isn't needed? 
//						result.onErrorResume(err -> {
//							AttemptContextReactiveAccessor.getLogger(ctx).info(ctx.attemptId(),
//									"caught exception '%s' in async, rethrowing", err);
//							return Mono.error(ctx.TransactionOperationFailedException.convertToOperationFailedIfNeeded(err, ctx));
//						}).thenReturn(ctx);

						return result.then(Mono.just(synchronizationManager));
					});
			/* begin spring-data-couchbase transaction  2/2 */  // this doesn't create a context on the desired publisher
			return sync.contextWrite(TransactionContextManager.getOrCreateContext())
					.contextWrite(TransactionContextManager.getOrCreateContextHolder()).then();
			/* end spring-data-couchbase transaction 2/2 */
		}).doOnSubscribe(v -> startTime.set(System.nanoTime()));

		txnResult.block();
		return execResult.get(); // transactions.reactive().executeTransaction(merged,overall,ob).doOnNext(v->overall.span().finish()).doOnError(err->overall.span().failWith(err));});

	}

	private void setTransaction(ReactiveCouchbaseTransactionManager.ReactiveCouchbaseTransactionObject transaction) {
		this.transaction = transaction;
	}

	@Override
	protected ReactiveCouchbaseTransactionManager.ReactiveCouchbaseTransactionObject doGetTransaction()
			throws TransactionException {
		/*
		CouchbaseResourceHolder resourceHolder = (CouchbaseResourceHolder) TransactionSynchronizationManager
				.getResource(template.getCouchbaseClientFactory());
		return new CouchbaseTransactionManager.CouchbaseTransactionObject(resourceHolder);
		*/
		return (ReactiveCouchbaseTransactionManager.ReactiveCouchbaseTransactionObject) transaction;
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
		org.springframework.transaction.support.TransactionSynchronizationManager
				.unbindResource(reactiveCouchbaseClientFactory);
	}

	@Override
	protected void doCleanupAfterCompletion(Object transaction) {
		LOGGER.trace("Performing cleanup of Couchbase Transaction {}", transaction);
		org.springframework.transaction.support.TransactionSynchronizationManager
				.unbindResource(reactiveCouchbaseClientFactory);
		return;
	}

	@Override
	public void destroy() {
	}

	@Override
	public Object getResourceFactory() {
		return reactiveTemplate.getCouchbaseClientFactory();
	}

	private static CouchbaseTransactionObject extractTransaction(Object transaction) {
		Assert.isInstanceOf(CouchbaseTransactionObject.class, transaction,
				() -> String.format("Expected to find a %s but it turned out to be %s.", CouchbaseTransactionObject.class,
						transaction.getClass()));

		return (CouchbaseTransactionObject) transaction;
	}
	/*
	public class CouchbaseResourceHolder extends ResourceHolderSupport {
	
	  private volatile ReactiveTransactionAttemptContext attemptContext;
	  //private volatile TransactionResultMap resultMap = new TransactionResultMap(template);
	
	  public CouchbaseResourceHolder(ReactiveTransactionAttemptContext attemptContext) {
	    this.attemptContext = attemptContext;
	  }
	
	  public ReactiveTransactionAttemptContext getAttemptContext() {
	    return attemptContext;
	  }
	
	  public void setAttemptContext(ReactiveTransactionAttemptContext attemptContext) {
	    this.attemptContext = attemptContext;
	  }
	
	  //public TransactionResultMap getTxResultMap() {
	  //  return resultMap;
	  //}
	
	  @Override
	  public String toString() {
	    return "CouchbaseResourceHolder{" + "attemptContext=" + attemptContext + "}";
	  }
	}
	
	 */

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

	private static Duration now() {
		return Duration.of(System.nanoTime(), ChronoUnit.NANOS);
	}

}
