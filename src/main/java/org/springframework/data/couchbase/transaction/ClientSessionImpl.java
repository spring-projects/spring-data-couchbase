package org.springframework.data.couchbase.transaction;

import com.couchbase.transactions.AttemptContext;
import com.couchbase.transactions.AttemptContextReactiveAccessor;
import com.couchbase.transactions.error.external.TransactionOperationFailed;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.transaction.reactive.AbstractReactiveTransactionManager;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.reactivestreams.Publisher;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.transactions.AttemptContextReactive;
import com.couchbase.transactions.TransactionContext;
import com.couchbase.transactions.TransactionGetResult;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.config.MergedTransactionConfig;
import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.support.AttemptStates;

public class ClientSessionImpl implements ClientSession {

	protected transient Log logger = LogFactory.getLog(AbstractReactiveTransactionManager.class);

	Mono<Scope> scopeRx;
	Scope scope;
	boolean commitInProgress = false;
	boolean messageSentInCurrentTransaction = true; // needs to be true for commit
	AttemptStates transactionState = AttemptStates.NOT_STARTED;
	TransactionOptions transactionOptions;
	Transactions transactions;
	TransactionContext ctx;
	TransactionConfig config;
	AttemptContextReactive atr = null;
	AttemptContext at = null;
	Map<Integer, TransactionResultHolder> getResultMap = new HashMap<>();

	public ClientSessionImpl(){}

	public ClientSessionImpl(ReactiveCouchbaseClientFactory couchbaseClientFactory, Transactions transactions,
													 TransactionConfig config, AttemptContextReactive atr) {
		this.transactions = transactions;
		scopeRx = couchbaseClientFactory.getScope();
		this.config = config == null
				? TransactionConfigBuilder.create().expirationTime(Duration.ofSeconds(120)).build()
				: config;
		MergedTransactionConfig merged = new MergedTransactionConfig(this.config, Optional.empty());
		ClusterEnvironment environment = couchbaseClientFactory.getCluster().block().environment();
		ctx = new TransactionContext(environment.requestTracer(), environment.eventBus(), UUID.randomUUID().toString(),
				now(), Duration.ZERO, merged);
		// does this not need an non-reactive AttemptContext?
		this.atr = atr;
	}

	public ClientSessionImpl(CouchbaseClientFactory couchbaseClientFactory, Transactions transactions,
													 TransactionConfig config, AttemptContext at) {
		this.transactions = transactions;
		scope = couchbaseClientFactory.getScope();
		this.config = config == null
				? TransactionConfigBuilder.create().expirationTime(Duration.ofSeconds(120)).build()
				: config;
		MergedTransactionConfig merged = new MergedTransactionConfig(this.config, Optional.empty());
		ClusterEnvironment environment = couchbaseClientFactory.getCluster().environment();
		ctx = new TransactionContext(environment.requestTracer(), environment.eventBus(), UUID.randomUUID().toString(),
				now(), Duration.ZERO, merged);
		this.at = at;
		if(at != null){
			this.atr = AttemptContextReactiveAccessor.getACR(at);
		}
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

	//@Override
	//public void setAttemptContextReactive(AttemptContextReactive atr){
	//	this.atr = atr;
	//}

	@Override
	public AttemptContextReactive getAttemptContextReactive(){
		return atr;
	}

	@Override
	public AttemptContext getAttemptContext(){
		return at;
	}


	// setter that returns `this`
	//@Override
	//public ClientSession with(AttemptContextReactive atr){
	//	setAttemptContextReactive(atr);
	//	return this;
	//}

	@Override
	public TransactionOptions getTransactionOptions() {
		return transactionOptions;
	}

	@Override
	public AsyncCluster getWrapped() {
		return null;
	}

	@Override
	public void startTransaction() {
		transactionState = AttemptStates.PENDING;
	}

	@Override
	public void startTransaction(TransactionConfig var1) {
		startTransaction();
	}

	@Override
	public Publisher<Void> commitTransaction() {
		if (this.transactionState == AttemptStates.ABORTED) {
			throw new IllegalStateException("Cannot call commitTransaction after calling abortTransaction");
		} else if (this.transactionState == AttemptStates.NOT_STARTED) {
			throw new IllegalStateException("There is no transaction started");
		} else if (!this.messageSentInCurrentTransaction) { // seems there should have been a messageSent. We just do nothing(?)
			this.cleanupTransaction(AttemptStates.COMMITTED);
			return Mono.create(MonoSink::success);
		} else {
			/*ReadConcern readConcern = this.transactionOptions.getReadConcern(); */
			if (0 == 1/* readConcern == null*/) {
				throw new CouchbaseException("Invariant violated. Transaction options read concern can not be null");
			} else {
				boolean alreadyCommitted = this.commitInProgress || this.transactionState == AttemptStates.COMMITTED;
				this.commitInProgress = true;
				// this will fail with ctx.serialized() being Optional.empty()
				// how does the commit happen in transactions.reactive().run() ?
				/*
				return transactions.reactive().commit(ctx.serialized().get(), null).then().doOnSuccess(x -> {
					commitInProgress = false;
					this.transactionState = AttemptStates.COMMITTED;
				}).doOnError(CouchbaseException.class, this::clearTransactionContextOnError);
				*/
				// TODO MSR
				// return Mono.create(MonoSink::success);
				return executeImplicitCommit(atr).then(); //transactions.reactive().executeImplicitCommit(atr).then();
				/*
				return this.executor.execute((new CommitTransactionOperation(this.transactionOptions.getWriteConcern(), alreadyCommitted)).recoveryToken(this.getRecoveryToken()).maxCommitTime(this.transactionOptions.getMaxCommitTime(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS), readConcern, this).doOnTerminate(() -> {
				  this.commitInProgress = false;
				  this.transactionState = AttemptStates.COMMITTED;
				}).doOnError(CouchbaseException.class, this::clearTransactionContextOnError);
				
				 */
			}

		}
	}

	public Mono<AttemptContextReactive> executeImplicitCommit(AttemptContextReactive ctx) {
		return Mono.defer(() -> {
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("About to commit ctx %s",	ctx));
			}
			// If app has not explicitly performed a commit, assume they want to do so anyway
			if (0 != 1 /*!ctx.isDone()*/) {
				if (0 == 1 /*ctx.serialized().isPresent()*/) {
					return Mono.just(ctx);
				} else {
					//System.err.println(ctx.attemptId()+ " doing implicit commit"); // ctx.LOGGER.trace();
					System.err.println("doing implicit commit");
					if(ctx != null) {
						return ctx.commit()
								.then(Mono.just(ctx))
								.onErrorResume(err -> Mono.error(TransactionOperationFailed.convertToOperationFailedIfNeeded(err,
										ctx)));
					} else {
						at.commit();
						return Mono.empty();
					}
				}
			} else {
				System.err.println("Transaction already done");
				System.err.println(ctx.attemptId()+" Transaction already done"); // // ctx.LOGGER.trace();
				return Mono.just(ctx);
			}
		});
	}



	@Override
	public Publisher<Void> abortTransaction() {
		System.err.println("**** abortTransaction ****");
		Assert.notNull(transactions, "transactions");
		Assert.notNull(ctx, "ctx");
		Assert.notNull(ctx.serialized(), "ctx.serialized()");
		if (ctx.serialized().isPresent()) {
			Assert.notNull(ctx.serialized().get(), "ctx.serialized().get()");
			return transactions.reactive().rollback(ctx.serialized().get(), null).then();
		} else {
			return  executeExplicitRollback(atr).then();
		}
	}

	private Mono<Void> executeExplicitRollback(AttemptContextReactive atr) {
		if(at != null){
			at.rollback();
			return Mono.empty();
		} else {
			return atr.rollback();
		}
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

	private void cleanupTransaction(AttemptStates attempState) {}

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
		//TransactionResultHolder holder = new TransactionResultHolder(result);
		System.err.println("PUT: "+System.identityHashCode(o)+" "+o);
		getResultMap.put(System.identityHashCode(o), holder);
		return holder;
	}

	private static Duration now() {
		return Duration.of(System.nanoTime(), ChronoUnit.NANOS);
	}
}
