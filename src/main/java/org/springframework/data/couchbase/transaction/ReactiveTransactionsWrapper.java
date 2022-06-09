package org.springframework.data.couchbase.transaction;

import reactor.core.publisher.Mono;

import java.util.function.Function;

import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;

import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;

public class ReactiveTransactionsWrapper {
	ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory;

	public ReactiveTransactionsWrapper(ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory) {
		this.reactiveCouchbaseClientFactory = reactiveCouchbaseClientFactory;
	}

	public Mono<TransactionResult> run(Function<ReactiveSpringTransactionAttemptContext, Mono<?>> transactionLogic) {
		return run(transactionLogic, null);
	}

	public Mono<TransactionResult> run(Function<ReactiveSpringTransactionAttemptContext, Mono<?>> transactionLogic,
			TransactionOptions perConfig) {
		Function<ReactiveTransactionAttemptContext, Mono<?>> newTransactionLogic = (ctx) -> {

			return transactionLogic.apply(new ReactiveSpringTransactionAttemptContext(ctx))

					// This reactive context is what tells Spring operations they're inside a transaction.
					.contextWrite(reactiveContext -> {
						CouchbaseResourceHolder resourceHolder = reactiveCouchbaseClientFactory.getResources(
								printThrough("core: ",AttemptContextReactiveAccessor.getCore(ctx)));
						return reactiveContext.put(CouchbaseResourceHolder.class, resourceHolder);
					});
		};

		return reactiveCouchbaseClientFactory.getCluster().reactive().transactions().run(newTransactionLogic,
				perConfig);

	}

	private static <T> T printThrough(String label, T obj){
		System.err.println(label+obj);
		return obj;
	}
}
