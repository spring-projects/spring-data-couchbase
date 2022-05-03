package org.springframework.data.couchbase.transaction;

import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionGetResult;
import com.couchbase.client.java.transactions.TransactionResult;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.repository.DynamicProxyable;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.CouchbaseException;

public class CouchbaseStuffHandle {

	// package org.springframework.transaction.reactive;
	private static final Log logger = LogFactory.getLog(CouchbaseStuffHandle.class);
	private final ReactiveTransactionManager transactionManager;
	private final TransactionDefinition transactionDefinition;

	Map<Integer, TransactionResultHolder> getResultMap = new HashMap<>();
	private ReactiveTransactionAttemptContext attemptContextReactive;

	public CouchbaseStuffHandle() {
		transactionManager = null;
		transactionDefinition = null;
	}

	public CouchbaseStuffHandle(ReactiveCouchbaseTransactionManager transactionManager) {
		this(transactionManager, new CouchbaseTransactionDefinition());
	}

	public CouchbaseStuffHandle(ReactiveCouchbaseTransactionManager transactionManager,
															TransactionDefinition transactionDefinition) {
		Assert.notNull(transactionManager, "ReactiveTransactionManager must not be null");
		Assert.notNull(transactionDefinition, "TransactionDefinition must not be null");
		this.transactionManager = transactionManager;
		this.transactionDefinition = transactionDefinition;
	}

	public Mono<TransactionResult> reactive(Function<CouchbaseStuffHandle, Mono<Void>> transactionLogic) {
		return reactive(transactionLogic, true);
	}

	/**
	 * A convenience wrapper around {@link TransactionsReactive#run}, that provides a default
	 * <code>PerTransactionConfig</code>.
	 */
	public Mono<TransactionResult> reactive(Function<CouchbaseStuffHandle, Mono<Void>> transactionLogic,
											boolean commit) {
		// todo gp this needs access to a Cluster
		return Mono.empty();
//		return ((ReactiveCouchbaseTransactionManager) transactionManager).getTransactions().reactive((ctx) -> {
//			setAttemptContextReactive(ctx); // for getTxOp().getCtx() in Reactive*OperationSupport
//			// for transactional(), transactionDefinition.setAtr(ctx) is called at the beginning of that method
//			// and is eventually added to the ClientSession in transactionManager.doBegin() via newResourceHolder()
//			return transactionLogic.apply(this);
//		}/*, commit*/);
	}

	public TransactionResultHolder transactionResultHolder(Integer key) {
		return getResultMap.get(key);
	}

	public TransactionResultHolder transactionResultHolder(TransactionGetResult result) {
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

	public ReactiveTransactionManager getTransactionManager() {
		return transactionManager;
	}

	public ReactiveCouchbaseTemplate template(ReactiveCouchbaseTemplate template) {
		ReactiveCouchbaseTransactionManager txMgr = ((ReactiveCouchbaseTransactionManager) ((CouchbaseStuffHandle) this)
				.getTransactionManager());
		if (template.getCouchbaseClientFactory() != txMgr.getDatabaseFactory()) {
			throw new CouchbaseException(
					"Template must use the same clientFactory as the transactionManager of the transactionalOperator "
							+ template);
		}
		return template;// .with(this); // template with a new couchbaseClient with txOperator
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
		ReactiveCouchbaseTransactionManager txMgr = ((ReactiveCouchbaseTransactionManager) ((CouchbaseStuffHandle) this)
				.getTransactionManager());

		if (reactiveOperations.getCouchbaseClientFactory() != txMgr.getDatabaseFactory()) {
			throw new CouchbaseException(
					"Repository must use the same clientFactory as the transactionManager of the transactionalOperator " + repo);
		}
		return repo.withTransaction(this); // this returns a new repository proxy with txOperator in its threadLocal
		// what if instead we returned a new repo with a new template with the txOperator?
	}

}
