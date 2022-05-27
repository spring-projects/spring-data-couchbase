package org.springframework.data.couchbase.transaction;

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionGetResult;
import com.couchbase.client.java.transactions.TransactionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.reactive.TransactionCallback;
import org.springframework.transaction.reactive.TransactionContextManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.repository.DynamicProxyable;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.CouchbaseException;

/**
 * What's this for again?
 * A transaction-enabled operator that uses the CouchbaseStuffHandle txOp instead of
 * what it finds in the currentContext()?
 *
 */
// todo gpx ongoing discussions on whether this can support retries & error handling natively
public class CouchbaseTransactionalOperator implements TransactionalOperator {

	// package org.springframework.transaction.reactive;
	private static final Logger logger = LoggerFactory.getLogger(CouchbaseTransactionalOperator.class);
	private final ReactiveTransactionManager transactionManager;
	private final TransactionDefinition transactionDefinition;

	Map<Integer, TransactionResultHolder> getResultMap = new HashMap<>();
	private ReactiveTransactionAttemptContext attemptContextReactive;

	public CouchbaseTransactionalOperator() {
		transactionManager = null;
		transactionDefinition = null;
	}

	public CouchbaseTransactionalOperator(ReactiveCouchbaseTransactionManager transactionManager) {
		this(transactionManager, new CouchbaseTransactionDefinition());
	}

	public ReactiveCouchbaseTemplate getTemplate(){
		return ((ReactiveCouchbaseTransactionManager)transactionManager).getDatabaseFactory().getTransactionalOperator()
				.getTemplate();
	}
	public CouchbaseTransactionalOperator(ReactiveCouchbaseTransactionManager transactionManager,
																				TransactionDefinition transactionDefinition) {
		Assert.notNull(transactionManager, "ReactiveTransactionManager must not be null");
		Assert.notNull(transactionDefinition, "TransactionDefinition must not be null");
		this.transactionManager = transactionManager;
		this.transactionDefinition = transactionDefinition;
	}

	public Mono<TransactionResult> reactive(Function<CouchbaseTransactionalOperator, Mono<Void>> transactionLogic) {
		return reactive(transactionLogic, true);
	}

	public TransactionResult run(Function<CouchbaseTransactionalOperator, Object> transactionLogic) {
		return reactive(new Function<CouchbaseTransactionalOperator, Mono<Void>>() {
											@Override
											public Mono<Void> apply(CouchbaseTransactionalOperator couchbaseTransactionalOperator) {
												return Mono.defer(() -> {transactionLogic.apply( couchbaseTransactionalOperator); return Mono.empty();});
											}
										},
				true).block();
	}


	/**
	 * A convenience wrapper around {@link TransactionsReactive#run}, that provides a default
	 * <code>PerTransactionConfig</code>.
	 */
	public Mono<TransactionResult> reactive(Function<CouchbaseTransactionalOperator, Mono<Void>> transactionLogic,
											boolean commit) {
//		// todo gp this needs access to a Cluster
//		return Mono.empty();
		return ((ReactiveCouchbaseTransactionManager) transactionManager).getDatabaseFactory().getCluster().block().reactive().transactions().run(ctx -> {
			setAttemptContextReactive(ctx); // for getTxOp().getCtx() in Reactive*OperationSupport
			// for transactional(), transactionDefinition.setAtr(ctx) is called at the beginning of that method
			// and is eventually added to the ClientSession in transactionManager.doBegin() via newResourceHolder()
			return transactionLogic.apply(this);
		}/*, commit*/);
	}

	public TransactionResultHolder transactionResultHolder(Integer key) {
		return getResultMap.get(key);
	}

	public TransactionResultHolder transactionResultHolder(CoreTransactionGetResult result) {
		TransactionResultHolder holder = new TransactionResultHolder(result);
		getResultMap.put(System.identityHashCode(holder), holder);
		return holder;
	}

	public void setAttemptContextReactive(ReactiveTransactionAttemptContext attemptContextReactive) {
		this.attemptContextReactive = attemptContextReactive;
		// see ReactiveCouchbaseTransactionManager.doBegin()
		// transactionManager.getReactiveTransaction(new CouchbaseTransactionDefinition()).block();
	//	CouchbaseResourceHolder holder = null;
	//TransactionSynchronizationManager.bindResource(((ReactiveCouchbaseTransactionManager)transactionManager).getDatabaseFactory(), holder);

		/*
		for savePerson that,  doBegin() is called from AbstractReactiveTransactionManager.getReactiveTransaction()
		which is called from TransactionalOperatorImpl.transactional(Mono<T>)
		[also called from TransactionalOperatorImpl.execute(TransactionCallback<T>)]
		 */
	}

	public ReactiveTransactionAttemptContext getAttemptContextReactive() {
		return attemptContextReactive;
	}

	public CoreTransactionAttemptContext getAttemptContext() {
		return AttemptContextReactiveAccessor.getCore(attemptContextReactive);
	}


	public ReactiveTransactionManager getTransactionManager() {
		return transactionManager;
	}

	public ReactiveCouchbaseTemplate template(ReactiveCouchbaseTemplate template) {
		ReactiveCouchbaseTransactionManager txMgr = ((ReactiveCouchbaseTransactionManager) ((CouchbaseTransactionalOperator) this)
				.getTransactionManager());
		if (template.getCouchbaseClientFactory() != txMgr.getDatabaseFactory()) {
			throw new CouchbaseException(
					"Template must use the same clientFactory as the transactionManager of the transactionalOperator "
							+ template);
		}
		return template.with(this); // template with a new couchbaseClient with txOperator
	}

	/*
	public CouchbaseTemplate template(CouchbaseTemplate template) {
		CouchbaseTransactionManager txMgr = ((CouchbaseTransactionManager) ((CouchbaseStuffHandle) this)
				.getTransactionManager());
		if (template.getCouchbaseClientFactory() != txMgr.getDatabaseFactory()) {
			throw new CouchbaseException(
					"Template must use the same clientFactory as the transactionManager of the transactionalOperator "
							+ template);
		}
		return template.with(this); // template with a new couchbaseClient with txOperator
	}
*/
	public <R extends DynamicProxyable<R>> R repository(R repo) {
		if (!(repo.getOperations() instanceof ReactiveCouchbaseOperations)) {
			throw new CouchbaseException("Repository must be a Reactive Couchbase repository" + repo);
		}
		ReactiveCouchbaseOperations reactiveOperations = (ReactiveCouchbaseOperations) repo.getOperations();
		ReactiveCouchbaseTransactionManager txMgr = (ReactiveCouchbaseTransactionManager) this.getTransactionManager();

		if (reactiveOperations.getCouchbaseClientFactory() != txMgr.getDatabaseFactory()) {
			throw new CouchbaseException(
					"Repository must use the same clientFactory as the transactionManager of the transactionalOperator " + repo);
		}
		return repo.withTransaction(this); // this returns a new repository proxy with txOperator in its threadLocal
		// what if instead we returned a new repo with a new template with the txOperator?
	}

	@Override
	public <T> Flux<T> transactional(Flux<T> flux) {
		return execute(it -> flux);
	}

	@Override
	public <T> Mono<T> transactional(Mono<T> mono) {
		return TransactionContextManager.currentContext().flatMap(context -> {
			// getCtx()/getAttemptTransActionReactive() has the atr
			// atr : transactionalOpterator -> transactionDefinition -> transactionHolder ->
			((CouchbaseTransactionDefinition) transactionDefinition).setAttemptContextReactive(getAttemptContextReactive());
			Mono<ReactiveTransaction> status = this.transactionManager.getReactiveTransaction(this.transactionDefinition);
			// This is an around advice: Invoke the next interceptor in the chain.
			// This will normally result in a target object being invoked.
			// Need re-wrapping of ReactiveTransaction until we get hold of the exception
			// through usingWhen.
			return status
					.flatMap(it -> Mono
							.usingWhen(Mono.just(it), ignore -> mono, this.transactionManager::commit, (res, err) -> { System.err.println("!!!!!!!!!! "+err+" "+res); return Mono.empty();},
									this.transactionManager::rollback)
							.onErrorResume(ex -> rollbackOnException(it, ex).then(Mono.error(ex))));
		}).contextWrite(TransactionContextManager.getOrCreateContext())
				.contextWrite(TransactionContextManager.getOrCreateContextHolder());
	}

	@Override
	public <T> Flux<T> execute(TransactionCallback<T> action) throws TransactionException {
		return TransactionContextManager.currentContext().flatMapMany(context -> {
			Mono<ReactiveTransaction> status = this.transactionManager.getReactiveTransaction(this.transactionDefinition);
			// This is an around advice: Invoke the next interceptor in the chain.
			// This will normally result in a target object being invoked.
			// Need re-wrapping of ReactiveTransaction until we get hold of the exception
			// through usingWhen.
			return status
					.flatMapMany(it -> Flux
							.usingWhen(Mono.just(it), action::doInTransaction, this.transactionManager::commit,
									(tx, ex) -> Mono.empty(), this.transactionManager::rollback)
							.onErrorResume(ex -> rollbackOnException(it, ex).then(Mono.error(ex))));
		}).contextWrite(TransactionContextManager.getOrCreateContext())
				.contextWrite(TransactionContextManager.getOrCreateContextHolder());
	}

	private Mono<Void> rollbackOnException(ReactiveTransaction status, Throwable ex) throws TransactionException {
		logger.debug("Initiating transaction rollback on application exception", ex);
		return this.transactionManager.rollback(status).onErrorMap((ex2) -> {
			logger.error("Application exception overridden by rollback exception", ex);
			if (ex2 instanceof TransactionSystemException) {
				((TransactionSystemException) ex2).initApplicationException(ex);
			}
			return ex2;
		});
	}

}
