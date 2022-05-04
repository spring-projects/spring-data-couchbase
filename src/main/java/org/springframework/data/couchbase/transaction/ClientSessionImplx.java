package org.springframework.data.couchbase.transaction;

import com.couchbase.client.core.transaction.support.AttemptState;
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.Transactions;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.transaction.reactive.AbstractReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionContext;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.reactivestreams.Publisher;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.env.ClusterEnvironment;

public class ClientSessionImplx implements ClientSessionx {

	protected transient Log logger = LogFactory.getLog(AbstractReactiveTransactionManager.class);

	Mono<Scope> scopeRx;
	Scope scope;
	boolean commitInProgress = false;
	boolean messageSentInCurrentTransaction = true; // needs to be true for commit
	// todo gp probably should not be duplicating CoreTransactionAttemptContext state outside of it
	//AttemptState transactionState = AttemptState.NOT_STARTED;
	TransactionOptions transactionOptions;
	TransactionContext ctx;
	ReactiveTransactionAttemptContext atr = null;
	Map<Integer, TransactionResultHolder> getResultMap = new HashMap<>();

	public ClientSessionImplx(){}

	public ClientSessionImplx(ReactiveCouchbaseClientFactory couchbaseClientFactory,  ReactiveTransactionAttemptContext atr) {
		this.scopeRx = couchbaseClientFactory.getScope();
		this.atr = atr;
		System.err.println("new "+this);
	}

	public ClientSessionImplx(CouchbaseClientFactory couchbaseClientFactory, ReactiveTransactionAttemptContext atr) {
		this.scope = couchbaseClientFactory.getScope();
		this.atr = atr;
		System.err.println("NEW "+this);
	}

	private Transactions getTransactions(Transactions transactions) {
		return transactions;
	}

	@Override
	public Mono<Scope> getScope() {
		return scopeRx;
	}

	@Override
	public boolean hasActiveTransaction() {
		return false;
	}

	@Override
	public boolean notifyMessageSent() {
		return false;
	}

	@Override
	public void notifyOperationInitiated(Object var1) {

	}

	@Override
	public ReactiveTransactionAttemptContext getReactiveTransactionAttemptContext(){
		return atr;
	}

	@Override
	public TransactionAttemptContext getTransactionAttemptContext(){
		return atr == null? null : AttemptContextReactiveAccessor.blocking(atr);
	}

	@Override
	public TransactionOptions getTransactionOptions() {
		return transactionOptions;
	}

	@Override
	public AsyncCluster getWrapped() {
		return null;
	}

	// todo gp
	@Override
	public void startTransaction() {
		System.err.println("startTransaction: "+this);
		//transactionState = AttemptState.PENDING;
	}

	// todo gp
	@Override
	public Publisher<Void> commitTransaction() {
		AttemptState state = getState();
		if (state == AttemptState.ABORTED) {
			throw new IllegalStateException("Cannot call commitTransaction after calling abortTransaction");
		} else if (state == AttemptState.NOT_STARTED) {
			throw new IllegalStateException("There is no transaction started");
		} else if (!this.messageSentInCurrentTransaction) { // seems there should have been a messageSent. We just do nothing(?)
			this.cleanupTransaction(AttemptState.COMMITTED);
			return Mono.create(MonoSink::success);
		} else {
			/*ReadConcern readConcern = this.transactionOptions.getReadConcern(); */
			if (0 == 1/* readConcern == null*/) {
				throw new CouchbaseException("Invariant violated. Transaction options read concern can not be null");
			} else {
				boolean alreadyCommitted = this.commitInProgress || state == AttemptState.COMMITTED;
				this.commitInProgress = true;
				// this will fail with ctx.serialized() being Optional.empty()
				// how does the commit happen in transactions.reactive().run() ?
				/*
				return transactions.reactive().commit(ctx.serialized().get(), null).then().doOnSuccess(x -> {
					commitInProgress = false;
					this.transactionState = AttemptState.COMMITTED;
				}).doOnError(CouchbaseException.class, this::clearTransactionContextOnError);
				*/
				// TODO MSR
				// return Mono.create(MonoSink::success);
				return executeImplicitCommit(atr).then(); //transactions.reactive().executeImplicitCommit(atr).then();
				/*
				return this.executor.execute((new CommitTransactionOperation(this.transactionOptions.getWriteConcern(), alreadyCommitted)).recoveryToken(this.getRecoveryToken()).maxCommitTime(this.transactionOptions.getMaxCommitTime(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS), readConcern, this).doOnTerminate(() -> {
				  this.commitInProgress = false;
				  this.transactionState = AttemptState.COMMITTED;
				}).doOnError(CouchbaseException.class, this::clearTransactionContextOnError);
				
				 */
			}

		}
	}

	public Mono<Void> executeImplicitCommit(ReactiveTransactionAttemptContext ctx) {

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("About to commit ctx %s",	ctx));
			}
			// If app has not explicitly performed a commit, assume they want to do so anyway
			if (0 != 1 /*!ctx.isDone()*/) {
				if (0 == 1 /*ctx.serialized().isPresent()*/) {
					return  Mono.empty(); // Mono.just(ctx);
				} else {
					//System.err.println(ctx.attemptId()+ " doing implicit commit"); // ctx.LOGGER.trace();
					System.err.println("doing implicit commit: "+this);
					return AttemptContextReactiveAccessor.implicitCommit(atr, false);

					// todo gp ctx.commit() has gone in the SDK integration.  Do we need this logic though?
					//return Mono.empty();
//					if(ctx != null) {
//						return ctx.commit()
//								.then(Mono.just(ctx))
//								.onErrorResume(err -> Mono.error(TransactionOperationFailed.convertToOperationFailedIfNeeded(err,
//										ctx)));
//					} else {
//						at.commit();
//						return Mono.empty();
//					}
				}
			} else {
				System.err.println("Transaction already done");
				//System.err.println(ctx.attemptId()+" Transaction already done"); // // ctx.LOGGER.trace();
				return Mono.empty(); // Mono.just(ctx);
			}
	}



	@Override
	public Publisher<Void> abortTransaction() {
		System.err.println("**** abortTransaction ****");
//		Assert.notNull(transactions, "transactions");
//		Assert.notNull(ctx, "ctx");
//		Assert.notNull(ctx.serialized(), "ctx.serialized()");
//		if (ctx.serialized().isPresent()) {
//			Assert.notNull(ctx.serialized().get(), "ctx.serialized().get()");
//			return transactions.reactive().rollback(ctx.serialized().get(), null).then();
//		} else {
			return  executeExplicitRollback(atr).then();
//		}
	}

	private Mono<Void> executeExplicitRollback(ReactiveTransactionAttemptContext atr) {
		// todo gp ctx.rollback() is removed
		// todo mr - so what happens when the client requests that the tx be rolledback?
		// todo mr - does throwing an exception result in rollback?
		// todo mr - should an exception be thrown here on a request to rollback, when we can't do a rollback?
		return Mono.empty();
	}

	@Override
	public ServerSession getServerSession() {
		return null;
	}

	@Override
	public void close() {

	}

	@Override
	public Object getClusterTime() {
		return null;
	}

	@Override
	public Object isCausallyConsistent() {
		return null;
	}

	private void cleanupTransaction(AttemptState attempState) {}

	private void clearTransactionContext() {}

	private void clearTransactionContextOnError(CouchbaseException e) {
		String s = e.getMessage() != null ? e.getMessage().toLowerCase(Locale.ROOT) : null;
		if (s != null && (s.contains("transienttransactionerror") || s.contains("unknowntransactioncommitresult"))) {
			this.clearTransactionContext();
		}

	}

	@Override
	public TransactionResultHolder transactionResultHolder(Integer key) {
		TransactionResultHolder holder = getResultMap.get(key);
		if(holder == null){
			throw new RuntimeException("did not find transactionResultHolder for key="+key+" in session");
		}
		return holder;
	}

	@Override
	public TransactionResultHolder transactionResultHolder(TransactionResultHolder holder, Object o) {
		System.err.println("PUT: "+System.identityHashCode(o)+" "+o);
		getResultMap.put(System.identityHashCode(o), holder);
		return holder;
	}

	private static Duration now() {
		return Duration.of(System.nanoTime(), ChronoUnit.NANOS);
	}

	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(this.getClass().getSimpleName()+"@"+System.identityHashCode(this));
		sb.append("{");
		sb.append("atr: "+ ( atr == null ? null : atr.toString().replace("com.couchbase.client.java.transactions.","")));
		sb.append(", state: "+(atr == null ? null : getState()));
		sb.append("}");
		return sb.toString();
	}

	private AttemptState getState() {
		AttemptState state = AttemptContextReactiveAccessor.getState(atr);
		return state != null ? state : AttemptState.NOT_STARTED;
	}
}
