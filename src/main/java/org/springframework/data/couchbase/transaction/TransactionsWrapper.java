package org.springframework.data.couchbase.transaction;

import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionContextManager;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

// todo gp needed now Transactions has gone?
public class TransactionsWrapper {
  ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory;

  public TransactionsWrapper(ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory){
    this.reactiveCouchbaseClientFactory = reactiveCouchbaseClientFactory;
  }

  /**
   * A convenience wrapper around {@link TransactionsReactive#run}, that provides a default <code>PerTransactionConfig</code>.
   */
  public Mono<TransactionResult> reactive(Function<ReactiveTransactionAttemptContext, Mono<Void>> transactionLogic) {
    // TODO long duration for debugger
    Duration duration = Duration.ofMinutes(20);
    System.err.println("tx duration of "+duration);
    return run(transactionLogic, TransactionOptions.transactionOptions().timeout(duration));
  }

  public Mono<TransactionResult> run(Function<ReactiveTransactionAttemptContext, Mono<Void>> transactionLogic,
                                     TransactionOptions perConfig) {
    // todo gp this is duplicating a lot of logic from the core loop, and is hopefully not needed..
    return Mono.empty();
//    TransactionConfig config = TransactionConfigBuilder.create().build();
//
//    ClusterEnvironment env = ClusterEnvironment.builder().build();
//    return Mono.defer(() -> {
//      MergedTransactionConfig merged = new MergedTransactionConfig(config, Optional.of(perConfig));
//
//      TransactionContext overall =
//          new TransactionContext(env.requestTracer(),
//              env.eventBus(),
//              UUID.randomUUID().toString(),
//              now(),
//              Duration.ZERO,
//              merged);
//      AtomicReference<Long> startTime = new AtomicReference<>(0L);
//
//      Mono<ReactiveTransactionAttemptContext> ob = Mono.fromCallable(() -> {
//        String txnId = UUID.randomUUID().toString();
//        overall.LOGGER.info(configDebug(config, perConfig));
//        return transactions.reactive().createAttemptContext(overall, merged, txnId);
//      }).flatMap(ctx -> {
//
//        AttemptContextReactiveAccessor.getLogger(ctx).info("starting attempt %d/%s/%s",
//            overall.numAttempts(), ctx.transactionId(), ctx.attemptId());
//
///* begin spring-data-couchbase transaction 1/2 */
//        ClientSession clientSession = reactiveCouchbaseClientFactory // couchbaseClientFactory
//            .getSession(ClientSessionOptions.builder().causallyConsistent(true).build(), transactions, null, ctx);
//        ReactiveCouchbaseResourceHolder resourceHolder = new ReactiveCouchbaseResourceHolder(clientSession,
//            reactiveCouchbaseClientFactory);
//        Mono<TransactionSynchronizationManager> sync = TransactionContextManager.currentContext()
//            .map(TransactionSynchronizationManager::new).<TransactionSynchronizationManager>flatMap(synchronizationManager -> {
//              synchronizationManager.bindResource(reactiveCouchbaseClientFactory.getCluster().block(), resourceHolder);
//              prepareSynchronization(synchronizationManager, null, new CouchbaseTransactionDefinition());
///* end spring-data-couchbase transaction  1/2 */
//             Mono<Void> result = transactionLogic.apply(ctx);
//              result
//                  .onErrorResume(err -> {
//                    AttemptContextReactiveAccessor.getLogger(ctx).info(ctx.attemptId(), "caught exception '%s' in async, rethrowing", err);
//                    logElidedStacktrace(ctx, err);
//
//                    return Mono.error(TransactionOperationFailed.convertToOperationFailedIfNeeded(err, ctx));
//                  })
//                  .thenReturn(ctx);
//              return result.then(Mono.just(synchronizationManager));
//            });
// /* begin spring-data-couchbase transaction  2/2 */
//        return sync.contextWrite(TransactionContextManager.getOrCreateContext())
//            .contextWrite(TransactionContextManager.getOrCreateContextHolder()).then(Mono.just(ctx));
///* end spring-data-couchbase transaction 2/2 */
//      }).doOnSubscribe(v -> startTime.set(System.nanoTime()))
//          .doOnNext(v -> AttemptContextReactiveAccessor.getLogger(v).trace(v.attemptId(), "finished attempt %d in %sms",
//              overall.numAttempts(), (System.nanoTime() - startTime.get()) / 1_000_000));
//
//      return transactions.reactive().executeTransaction(merged, overall, ob)
//          .doOnNext(v -> overall.span().finish())
//          .doOnError(err -> overall.span().failWith(err));
//    });
  }

//  private  void logElidedStacktrace(ReactiveTransactionAttemptContext ctx, Throwable err) {
//    transactions.reactive().logElidedStacktrace(ctx, err);
//  }
//
//  private String configDebug(TransactionConfig config, PerTransactionConfig perConfig) {
//    return transactions.reactive().configDebug(config, perConfig);
//  }
//
  private static Duration now() {
    return Duration.of(System.nanoTime(), ChronoUnit.NANOS);
  }

  private static void prepareSynchronization(TransactionSynchronizationManager synchronizationManager,
                                             ReactiveTransaction status, TransactionDefinition definition) {

    // if (status.isNewSynchronization()) {
    synchronizationManager.setActualTransactionActive(false /*status.hasTransaction()*/);
    synchronizationManager.setCurrentTransactionIsolationLevel(
        definition.getIsolationLevel() != TransactionDefinition.ISOLATION_DEFAULT ? definition.getIsolationLevel()
            : null);
    synchronizationManager.setCurrentTransactionReadOnly(definition.isReadOnly());
    synchronizationManager.setCurrentTransactionName(definition.getName());
    synchronizationManager.initSynchronization();
    // }
  }

}
