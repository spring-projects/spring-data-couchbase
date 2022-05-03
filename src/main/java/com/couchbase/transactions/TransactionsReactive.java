/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.transactions;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.retry.reactor.DefaultRetry;
import com.couchbase.client.core.retry.reactor.Jitter;
import com.couchbase.client.core.retry.reactor.RetryContext;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.ReactiveScope;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.transactions.cleanup.ClusterData;
import com.couchbase.transactions.cleanup.TransactionsCleanup;
import com.couchbase.transactions.components.ATR;
import com.couchbase.transactions.components.ActiveTransactionRecord;
import com.couchbase.transactions.config.MergedTransactionConfig;
import com.couchbase.transactions.config.PerTransactionConfig;
import com.couchbase.transactions.config.PerTransactionConfigBuilder;
import com.couchbase.transactions.config.SingleQueryTransactionConfig;
import com.couchbase.transactions.config.SingleQueryTransactionConfigBuilder;
import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.deferred.TransactionSerializedContext;
import com.couchbase.transactions.error.TransactionCommitAmbiguous;
import com.couchbase.transactions.error.TransactionExpired;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.error.internal.ErrorClasses;
import com.couchbase.transactions.error.external.TransactionOperationFailed;
import com.couchbase.transactions.forwards.Supported;
import com.couchbase.transactions.log.EventBusPersistedLogger;
import com.couchbase.transactions.log.PersistedLogWriter;
import com.couchbase.transactions.log.TransactionLogEvent;
import com.couchbase.transactions.support.AttemptContextFactory;
import com.couchbase.transactions.support.AttemptStates;
import com.couchbase.transactions.support.OptionsWrapperUtil;
import com.couchbase.transactions.util.DebugUtil;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.couchbase.transactions.error.internal.TransactionOperationFailedBuilder.createError;
import static com.couchbase.transactions.log.PersistedLogWriter.MAX_LOG_ENTRIES_DEFAULT;
import static com.couchbase.transactions.support.SpanWrapperUtil.DB_COUCHBASE_TRANSACTIONS;

/**
 * An asynchronous version of {@link Transactions}, allowing transactions to be created and run in an asynchronous
 * manner.
 * <p>
 * The main method to run transactions is {@link TransactionsReactive#run}.
 */
public class TransactionsReactive {
    static final int MAX_ATTEMPTS = 1000;
    private final TransactionsCleanup cleanup;
    private final TransactionConfig config;
    private AttemptContextFactory attemptContextFactory;
    private EventBusPersistedLogger persistedLogger;

    /**
     * This is package-private.  Applications should create a {@link Transactions} object instead, and then call {@link
     * Transactions#reactive}.
     */
    static TransactionsReactive create(Cluster cluster, TransactionConfig config) {
        return new TransactionsReactive(cluster, config);
    }

    private TransactionsReactive(Cluster cluster, TransactionConfig config) {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(config);

        ClusterData clusterData = new ClusterData(cluster);
        this.config = config;
        this.attemptContextFactory = config.attemptContextFactory();
        MergedTransactionConfig merged = new MergedTransactionConfig(config, Optional.empty());
        cleanup = new TransactionsCleanup(merged, clusterData);

        config.persistentLoggingCollection().ifPresent(collection -> {
            PersistedLogWriter persistedLogWriter = new PersistedLogWriter(collection, MAX_LOG_ENTRIES_DEFAULT);
            persistedLogger = new EventBusPersistedLogger(cluster.environment().eventBus(), persistedLogWriter, merged);
        });
    }


    /**
     * The main transactions 'engine', responsible for attempting the transaction logic as many times as required,
     * until the transaction commits, is explicitly rolled back, or expires.
     */
    // TODO: changed from private to public.  package-protected plus an accessor would be ok to
    public Mono<TransactionResult> executeTransaction(MergedTransactionConfig config,
                                                       TransactionContext overall,
                                                       Mono<AttemptContextReactive> transactionLogic) {
        AtomicReference<Long> startTime = new AtomicReference<>();

        return Mono.just(overall)

                .subscribeOn(reactor.core.scheduler.Schedulers.elastic())

                .doOnSubscribe(v -> {
                    if (startTime.get() == null) startTime.set(System.nanoTime());
                })

                // Where the magic happens: execute the app's transaction logic
                // A AttemptContextReactive gets created in here.  Rollback requires one of these (so it knows what
                // to rollback), so only errors thrown inside this block can trigger rollback.
                // So, expiry checks only get done inside this block.
                .then(transactionLogic)

                .flatMap(this::executeImplicitCommit)

                // Track an attempt if non-error, and request that the attempt be cleaned up.  Similar logic is also
                // done in executeHandleErrorsPreRetry.
                .doOnNext(ctx -> executeAddAttemptAndCleanupRequest(config, overall, ctx))

                // Track an attempt if error, and perform rollback if needed.
                // All errors reaching here must be a `TransactionOperationFailed`.
                .onErrorResume(err -> executeHandleErrorsPreRetry(config, overall, err))

                // This is the main place to retry txns.  Feed all errors up to this centralised point.
                // All errors reaching here must be a `TransactionOperationFailed`.
                .retryWhen(executeCreateRetryWhen(overall))

                // If we're here, then we've hit an error that we don't want to retry.
                // Either raise some derivative of TransactionFailed to the app, or return an AttemptContextReactive
                // to return success (some errors result in success, e.g. TRANSACTION_FAILED_POST_COMMIT)
                // All errors reaching here must be an `ErrorWrapper`.
                .onErrorResume(err -> executeHandleErrorsPostRetry(overall, err))

                .doOnError(err -> {
                    if (config.logOnFailure() && !config.logDirectly()) {
                        EventBus eventBus = cleanup.clusterData().cluster().environment().eventBus();
                        overall.LOGGER.logs().forEach(log -> {
                            eventBus.publish(new TransactionLogEvent(config.logOnFailureLevel(),
                                    TransactionLogEvent.DEFAULT_CATEGORY, log.toString()));
                        });
                    }
                })

                // If we get here, success
                .doOnSuccess(v ->
                        overall.LOGGER.info("finished txn in %dus",
                                TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime.get()))
                )

                // Safe to do single() as there will only ever be 1 result
                .single()
                .map(v -> createResultFromContext(overall));
    }

    private reactor.util.retry.Retry executeCreateRetryWhen(TransactionContext overall) {
        Predicate<? super RetryContext<Object>> predicate = context -> {
            Throwable exception = context.exception();

            if (!(exception instanceof TransactionOperationFailed)) {
                // A bug.  Only TransactionOperationFailed is allowed to reach here.
                throw new IllegalStateException("Non-TransactionOperationFailed '" + DebugUtil.dbg(exception) + "' received during retry, this is a bug", exception);
            }

            TransactionOperationFailed e = (TransactionOperationFailed) exception;

            overall.LOGGER.info("TransactionOperationFailed retryTransaction=%s", e.retryTransaction());

            return e.retryTransaction();
        };

        return DefaultRetry.create(predicate)

                .exponentialBackoff(Duration.of(1, ChronoUnit.MILLIS),
                        Duration.of(2, ChronoUnit.MILLIS))

                .doOnRetry(v -> overall.LOGGER.info("<>", "retrying transaction after backoff %dmillis", v.backoff().toMillis()))

                // Add some jitter so two txns don't livelock each other
                .jitter(Jitter.random())

                // Really, this is a safety-guard.  The txn will be aborted when it expires.
                .retryMax(MAX_ATTEMPTS)

                .toReactorRetry();
    }

    private Mono<AttemptContextReactive> executeHandleErrorsPreRetry(MergedTransactionConfig config,
                                                                     TransactionContext overall, Throwable err) {
        if (!(err instanceof TransactionOperationFailed)) {
            // A bug.  Only TransactionOperationFailed is allowed to reach here.
            overall.LOGGER.warn("<>", "received non-TransactionOperationFailed error %s, unable to rollback as don't have " +
                    "context", DebugUtil.dbg(err));
            return Mono.error(new IllegalStateException("received non-TransactionOperationFailed error " + err.getClass().getName() + " in pre-retry", err));
        }

        Mono<Void> autoRollback = Mono.empty();
        Mono<Void> cleanupReq = Mono.empty();

        TransactionOperationFailed e = (TransactionOperationFailed) err;
        AttemptContextReactive ctx = e.context();

        overall.LOGGER.info("<>", "finishing attempt off after error '%s'", e);

        if (e.autoRollbackAttempt()) {
            // In queryMode we always ROLLBACK, as there is possibly delta table state to cleanup, and there may be an
            // ATR - we don't know
            if (ctx.state() == AttemptStates.NOT_STARTED && !ctx.queryMode()) {
                // This is a better way of doing [RETRY-ERR-NOATR] and likely means that the older logic for
                // handling that won't trigger now
                ctx.LOGGER.info(ctx.attemptId(), "told to auto-rollback but in NOT_STARTED state, so nothing to do - skipping rollback");
            }
            else {
                ctx.LOGGER.info(ctx.attemptId(), "auto-rolling-back on error");

                autoRollback = ctx.rollbackInternal(false);
            }
        } else {
            ctx.LOGGER.info(ctx.attemptId(), "has been told to skip auto-rollback");
        }

        if (!config.runRegularAttemptsCleanupThread()) {
            // Don't add a request to a queue that no-one will be processing
            ctx.LOGGER.trace(ctx.attemptId(), "skipping addition of cleanup request on failure as regular cleanup disabled");
        }
        else {
            cleanupReq = Mono.fromRunnable(() -> addCleanupRequestForContext(ctx));
        }

        Mono<Void> addAttempt = Mono.fromRunnable(() -> {
            TransactionAttempt ta = TransactionAttempt.createFromContext(ctx, Optional.of(err));
            overall.addAttempt(ta);
            ctx.LOGGER.info(ctx.attemptId(), "added attempt %s after error", ta);
        });

        final Mono<Void> cleanupReqForLambda = cleanupReq;

        return autoRollback
                // See [Primary Operations] section in design document
                .onErrorResume(er -> {
                    overall.LOGGER.info("<>", "rollback failed with %s, raising original error but with retryTransaction turned off",
                            DebugUtil.dbg(er));

                    // Still want to add attempt and cleanup request
                    return cleanupReqForLambda
                            .then(addAttempt)
                            .then(Mono.error(createError(e.context(), e.causingErrorClass())
                                    .raiseException(e.toRaise())
                                    .cause(e.getCause())
                                    .build()));
                })
                .then(cleanupReqForLambda)
                // Only want to add the attempt after doing the rollback, so the attempt has the correct state (hopefully
                // ROLLED_BACK)
                .then(addAttempt)
                .then(Mono.defer(() -> {
                    if (e.retryTransaction() && overall.hasExpiredClientSide()) {
                        overall.LOGGER.info("<>", "original error planned to retry transaction, but it has subsequently expired");
                        return Mono.error(createError(ctx, ErrorClasses.FAIL_EXPIRY)
                                .doNotRollbackAttempt()
                                .raiseException(TransactionOperationFailed.FinalErrorToRaise.TRANSACTION_EXPIRED)
                                .build());
                    }
                    else {
                        // Raise the error up the stack so the logic later can decide whether to retry the transaction
                        overall.LOGGER.info("<>", "reraising original exception %s", DebugUtil.dbg(err));
                        return Mono.error(err);
                    }
                }))
                .doFinally(v -> ctx.span().failWith(e))
                .thenReturn(ctx);
    }

    private Mono<AttemptContextReactive> executeHandleErrorsPostRetry(TransactionContext overall, Throwable err) {
        if (!(err instanceof TransactionOperationFailed)) {
            // A bug.  Only TransactionOperationFailed is allowed to reach here.
            return Mono.error(new IllegalStateException("Non-TransactionOperationFailed '" + DebugUtil.dbg(err) + "' received, this is a bug"));
        }

        TransactionResult result = createResultFromContext(overall);
        TransactionOperationFailed e = (TransactionOperationFailed) err;

        if (e.toRaise() == TransactionOperationFailed.FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT) {
            e.context().LOGGER.info(e.context().attemptId(), "converted TRANSACTION_FAILED_POST_COMMIT to success, unstagingComplete() will be false");

            return Mono.just(e.context());
        }
        else {
            TransactionFailed ret;

            switch (e.toRaise()) {
                case TRANSACTION_EXPIRED: {
                    String msg = "Transaction has expired configured timeout of " + overall.expirationTime().toMillis() + "msecs.  The transaction is not committed.";
                    ret = new TransactionExpired(e.getCause(), result, msg);
                    break;
                }
                case TRANSACTION_COMMIT_AMBIGUOUS: {
                    String msg = "It is ambiguous whether the transaction committed";
                    ret = new TransactionCommitAmbiguous(e.getCause(), result, msg);
                    break;
                }
                default:
                    ret = new TransactionFailed(e.getCause(), result);
                    break;
            }

            e.context().LOGGER.info(e.context().attemptId(), "converted TransactionOperationFailed %s to final error %s",
                    e.toRaise(), ret);

            return Mono.error(ret);
        }
    }

    private void executeAddAttemptAndCleanupRequest(MergedTransactionConfig config, TransactionContext overall,
                                                    AttemptContextReactive ctx) {
        TransactionAttempt ta = TransactionAttempt.createFromContext(ctx, Optional.empty());
        overall.addAttempt(ta);
        ctx.LOGGER.info(ctx.attemptId(), "added attempt %s after success", ta);

        if (config.runRegularAttemptsCleanupThread()) {
            addCleanupRequestForContext(ctx);
        } else {
            ctx.LOGGER.trace(ctx.attemptId(), "skipping addition of cleanup request on success");
        }

        ctx.span().finish();
    }

    private Mono<AttemptContextReactive> executeImplicitCommit(AttemptContextReactive ctx) {
        return Mono.defer(() -> {
            // If app has not explicitly performed a commit, assume they want to do so anyway
            if (!ctx.isDone()) {
                if (ctx.serialized().isPresent()) {
                    return Mono.just(ctx);
                } else {
                    ctx.LOGGER.trace(ctx.attemptId(), "doing implicit commit");

                    return ctx.commit()
                            .then(Mono.just(ctx))
                            .onErrorResume(err -> Mono.error(TransactionOperationFailed.convertToOperationFailedIfNeeded(err, ctx)));
                }
            } else {
                return Mono.just(ctx);
            }
        });
    }

    // TODO: changed from package-protected to public (could have just used an accessor class in same package)
     AttemptContextReactive createAttemptContext(TransactionContext overall,
                                                MergedTransactionConfig config,
                                                String attemptId) {
        // null only happens in testing with Mockito, harmless
        if (overall != null) {
            return attemptContextFactory.create(overall, config, attemptId, this, Optional.of(overall.span()));
        } else {
            return null;
        }
    }

    /**
     * Runs the supplied transactional logic until success or failure.
     * <p>
     * This is the asynchronous version of {@link Transactions#run}, so to cover the differences:
     * <ul>
     * <li>The transaction logic is supplied with a {@link AttemptContextReactive}, which contains asynchronous
     * methods to allow it to read, mutate, insert and delete documents, as well as commit or rollback the
     * transactions.</li>
     * <li>The transaction logic should run these methods as a Reactor chain.</li>
     * <li>The transaction logic should return a <code>Mono{@literal <}Void{@literal >}</code>.  Any
     * <code>Flux</code> or <code>Mono</code> can be converted to a <code>Mono{@literal <}Void{@literal >}</code> by
     * calling <code>.then()</code> on it.</li>
     * <li>This method returns a <code>Mono{@literal <}TransactionResult{@literal >}</code>, which should be handled
     * as a normal Reactor Mono.</li>
     * </ul>
     *
     * @param transactionLogic the application's transaction logic
     * @param perConfig        the configuration to use for this transaction
     * @return there is no need to check the returned {@link TransactionResult}, as success is implied by the lack of a
     * thrown exception.  It contains information useful only for debugging and logging.
     * @throws TransactionFailed or a derived exception if the transaction fails to commit for any reason, possibly
     *                           after multiple retries.  The exception contains further details of the error.  Not
     */
    public Mono<TransactionResult> run(Function<AttemptContextReactive, Mono<Void>> transactionLogic,
                                       PerTransactionConfig perConfig) {
        return Mono.defer(() -> {
            MergedTransactionConfig merged = new MergedTransactionConfig(config, Optional.of(perConfig));

            TransactionContext overall =
                    new TransactionContext(cleanup.clusterData().cluster().environment().requestTracer(),
                            cleanup.clusterData().cluster().environment().eventBus(),
                            UUID.randomUUID().toString(),
                            now(),
                            Duration.ZERO,
                            merged);
            AtomicReference<Long> startTime = new AtomicReference<>(0L);

            Mono<AttemptContextReactive> ob = Mono.fromCallable(() -> {
                String txnId = UUID.randomUUID().toString();
                overall.LOGGER.info(configDebug(config, perConfig));
                return createAttemptContext(overall, merged, txnId);
            }).flatMap(ctx -> {
                ctx.LOGGER.info("starting attempt %d/%s/%s",
                        overall.numAttempts(), ctx.transactionId(), ctx.attemptId());
                Mono<Void> result = transactionLogic.apply(ctx);
                return result
                        .onErrorResume(err -> {
                            ctx.LOGGER.info(ctx.attemptId(), "caught exception '%s' in async, rethrowing", err);
                            logElidedStacktrace(ctx, err);

                            return Mono.error(TransactionOperationFailed.convertToOperationFailedIfNeeded(err, ctx));
                        })
                        .thenReturn(ctx);
            }).doOnSubscribe(v -> startTime.set(System.nanoTime()))
                    .doOnNext(v -> v.LOGGER.trace(v.attemptId(), "finished attempt %d in %sms",
                            overall.numAttempts(), (System.nanoTime() - startTime.get()) / 1_000_000));

            return executeTransaction(merged, overall, ob)
                    .doOnNext(v -> overall.span().finish())
                    .doOnError(err -> overall.span().failWith(err));
        });
    }

    // Printing the stacktrace is expensive in terms of log noise, but has been a life saver on many debugging
    // encounters.  Strike a balance by eliding the more useless elements.
    // TODO: changed from private to public
    public void logElidedStacktrace(AttemptContextReactive ctx, Throwable err) {
        DebugUtil.fetchElidedStacktrace(err, (s) -> ctx.LOGGER.info(ctx.attemptId(), "          " + s.toString()));
    }

    // TODO: changed from private to public
    public static String configDebug(TransactionConfig config, PerTransactionConfig perConfig) {
        StringBuilder sb = new StringBuilder();
        sb.append("library version: ");
        sb.append(TransactionsReactive.class.getPackage().getImplementationVersion());
        sb.append(" config: ");
        sb.append("atrs=");
        sb.append(config.numAtrs());
        sb.append(", metadataCollection=");
        sb.append(config.metadataCollection());
        sb.append(", expiry=");
        sb.append(perConfig.expirationTime().orElse(config.transactionExpirationTime()).toMillis());
        sb.append("msecs durability=");
        sb.append(config.durabilityLevel());
        sb.append(" per-txn config=");
        sb.append(" durability=");
        sb.append(perConfig.durabilityLevel());
        sb.append(", supported=");
        sb.append(Supported.SUPPORTED);
        return sb.toString();
    }

    /**
     * Convenience overload that runs {@link TransactionsReactive#run} with a default <code>PerTransactionConfig</code>.
     */
    public Mono<TransactionResult> run(Function<AttemptContextReactive, Mono<Void>> transactionLogic) {
        return run(transactionLogic, PerTransactionConfigBuilder.create().build());
    }

    @Stability.Volatile
    public Mono<TransactionResult> commit(TransactionSerializedContext serialized, PerTransactionConfig perConfig) {
        return deferred(serialized,
                perConfig,
                // Nothing to actually do, just want the implicit commit
                (ctx) -> Mono.empty());
    }

    @Stability.Volatile
    public Mono<TransactionResult> rollback(TransactionSerializedContext serialized, PerTransactionConfig perConfig) {
        return deferred(serialized,
                perConfig,
                (ctx) -> ctx.rollback());
    }

    @Stability.Volatile
    private Mono<TransactionResult> deferred(TransactionSerializedContext serialized,
                                             PerTransactionConfig perConfig,
                                             Function<AttemptContextReactive, Mono<Void>> initial) {
        MergedTransactionConfig merged = new MergedTransactionConfig(config, Optional.of(perConfig));
        JsonObject hydrated = JsonObject.fromJson(serialized.encodeAsString());

        String atrBucket = hydrated.getString("atrBucket");
        String atrScope = hydrated.getString("atrScope");
        String atrCollectionName = hydrated.getString("atrCollection");
        String atrId = hydrated.getString("atrId");
        ReactiveCollection atrCollection = cleanup.clusterData()
                .getBucketFromName(atrBucket)
                .scope(atrScope)
                .collection(atrCollectionName);

        return ActiveTransactionRecord.getAtr(atrCollection,
                                atrId,
                                OptionsWrapperUtil.kvTimeoutNonMutating(merged, atrCollection.core()),
                                null)

                .flatMap(atrOpt -> {
                    if (!atrOpt.isPresent()) {
                        return Mono.error(new IllegalStateException(String.format("ATR %s/%s could not be found",
                                atrBucket, atrId)));
                    }
                    else {
                        ATR atr = atrOpt.get();

                        // Note startTimeServerMillis is written with ${Mutation.CAS} while currentTimeServer
                        // could have come from $vbucket.HLC and is hence one-second granularity.  So, this is a
                        // somewhat imperfect comparison.
                        Duration currentTimeServer = Duration.ofNanos(atr.cas());
                        Duration startTimeServer = Duration.ofMillis(hydrated.getLong("startTimeServerMillis"));

                        // This includes the time elapsed during the first part of the transaction, plus any time
                        // elapsed during the period the transaction was expired.  Total time since the transaction
                        // began, basically.
                        Duration timeElapsed = currentTimeServer.minus(startTimeServer);

                        TransactionContext overall =
                                new TransactionContext(cleanup.clusterData().cluster().environment().requestTracer(),
                                        cleanup.clusterData().cluster().environment().eventBus(),
                                        UUID.randomUUID().toString(),
                                        Duration.ofNanos(System.nanoTime()),
                                        timeElapsed,
                                        merged);
                        AtomicReference<Long> startTime = new AtomicReference<>(0L);

                        overall.LOGGER.info("elapsed time = %dmsecs (ATR start time %dmsecs, current ATR time %dmsecs)",
                                timeElapsed.toMillis(), startTimeServer.toMillis(), currentTimeServer.toMillis());

                        Mono<AttemptContextReactive> ob = Mono.defer(() -> {
                            AttemptContextReactive ctx = attemptContextFactory.createFrom(hydrated, overall, merged, this);
                            ctx.LOGGER.info("starting attempt %d/%s/%s",
                                    overall.numAttempts(), ctx.transactionId(), ctx.attemptId());
                            ctx.LOGGER.info(configDebug(config, perConfig));

                            return initial.apply(ctx)

                                    // TXNJ-50: Make sure we run user's blocking logic on a scheduler that can take it
                                    .subscribeOn(Schedulers.elastic())

                                    .onErrorResume(err -> {
                                        ctx.LOGGER.info(ctx.attemptId(), "caught exception '%s' in deferred, rethrowing",
                                                err);

                                        logElidedStacktrace(ctx, err);

                                        return Mono.error(TransactionOperationFailed.convertToOperationFailedIfNeeded(err, ctx));
                                    })

                                    .doOnSubscribe(v -> startTime.set(System.nanoTime()))

                                    .doOnNext(v -> {
                                        ctx.LOGGER.trace(ctx.attemptId(), "finished attempt %d in %sms",
                                                overall.numAttempts(), (System.nanoTime() - startTime.get()) / 1_000_000);
                                    })

                                    .thenReturn(ctx);
                        });

                        return executeTransaction(merged, overall, ob)
                                .doOnNext(v -> overall.span().attribute(DB_COUCHBASE_TRANSACTIONS + "retries", overall.numAttempts()).finish())
                                .doOnError(err -> overall.span().attribute(DB_COUCHBASE_TRANSACTIONS + "retries", overall.numAttempts()).failWith(err));
                    }
                });
    }

    Mono<TransactionResult> runBlocking(Consumer<AttemptContext> txnLogic, PerTransactionConfig perConfig) {
        return Mono.defer(() -> {
            MergedTransactionConfig merged = new MergedTransactionConfig(config, Optional.of(perConfig));
            TransactionContext overall =
                    new TransactionContext(cleanup.clusterData().cluster().environment().requestTracer(),
                            cleanup.clusterData().cluster().environment().eventBus(),
                            UUID.randomUUID().toString(),
                            now(),
                            Duration.ZERO,
                            merged);
            AtomicReference<Long> startTime = new AtomicReference<>(0L);
            overall.LOGGER.info(configDebug(config, perConfig));

            Mono<AttemptContextReactive> ob = Mono.defer(() -> {
                String txnId = UUID.randomUUID().toString();
                AttemptContextReactive ctx = createAttemptContext(overall, merged, txnId);
                AttemptContext ctxBlocking = new AttemptContext(ctx);
                ctx.LOGGER.info("starting attempt %d/%s/%s",
                        overall.numAttempts(), ctx.transactionId(), ctx.attemptId());

                return Mono.fromRunnable(() -> txnLogic.accept(ctxBlocking))

                        // TXNJ-50: Make sure we run user's blocking logic on a scheduler that can take it
                        .subscribeOn(Schedulers.elastic())

                        .onErrorResume(err -> {
                            ctx.LOGGER.info(ctx.attemptId(), "caught exception '%s' in runBlocking, rethrowing", err);

                            logElidedStacktrace(ctx, err);

                            return Mono.error(TransactionOperationFailed.convertToOperationFailedIfNeeded(err, ctx));
                        })

                        .doOnSubscribe(v -> startTime.set(System.nanoTime()))

                        .doOnNext(v -> {
                            ctx.LOGGER.trace(ctx.attemptId(), "finished attempt %d in %sms",
                                    overall.numAttempts(), (System.nanoTime() - startTime.get()) / 1_000_000);
                        })

                        .thenReturn(ctx);
            });

            return executeTransaction(merged, overall, ob)
                    .doOnNext(v -> overall.span().attribute(DB_COUCHBASE_TRANSACTIONS + "retries", overall.numAttempts()).finish())
                    .doOnError(err -> overall.span().attribute(DB_COUCHBASE_TRANSACTIONS + "retries", overall.numAttempts()).failWith(err));
        });
    }

    public TransactionConfig config() {
        return config;
    }

    private static Duration now() {
        return Duration.of(System.nanoTime(), ChronoUnit.NANOS);
    }

    TransactionsCleanup cleanup() {
        return cleanup;
    }

    private void addCleanupRequestForContext(AttemptContextReactive ctx) {
        // Whether the txn was successful or not, still want to clean it up
        if (ctx.queryMode()) {
            ctx.LOGGER.info(ctx.attemptId(), "Skipping cleanup request as in query mode");
        }
        else if (ctx.serialized().isPresent()) {
            ctx.LOGGER.info(ctx.attemptId(), "Skipping cleanup request as deferred transaction");
        }
        else if (ctx.atrId().isPresent() && ctx.atrCollection().isPresent()) {
            switch (ctx.state()) {
                case NOT_STARTED:
                case COMPLETED:
                case ROLLED_BACK:
                    ctx.LOGGER.trace(ctx.attemptId(), "Skipping addition of cleanup request in state %s", ctx.state());
                    break;
                default:
                    ctx.LOGGER.trace(ctx.attemptId(), "Adding cleanup request for %s/%s",
                            ctx.atrCollection().get().name(), ctx.atrId().get());

                    cleanup.add(ctx.createCleanupRequest());
            }
        } else {
            // No ATR entry to remove
            ctx.LOGGER.trace(ctx.attemptId(), "Skipping cleanup request as no ATR entry to remove (due to no " +
                    "mutations)");
        }
    }

    private static TransactionResult createResultFromContext(TransactionContext overall) {
        return new TransactionResult(overall.attempts(),
                overall.LOGGER,
                Duration.of(System.nanoTime() - overall.startTimeClient().toNanos(), ChronoUnit.NANOS),
                overall.transactionId(),
                overall.serialized());
    }

    /**
     * Performs a single query transaction, with default configuration.
     *
     * @param statement the statement to execute.
     * @return a ReactiveSingleQueryTransactionResult
     */
    @Stability.Uncommitted
    public Mono<ReactiveSingleQueryTransactionResult> query(String statement) {
        return query(null, statement, SingleQueryTransactionConfigBuilder.create().build());
    }

    /**
     * Performs a single query transaction, with a custom configuration.
     *
     * @param statement the statement to execute.
     * @param queryOptions configuration options.
     * @return a ReactiveSingleQueryTransactionResult
     */
    @Stability.Uncommitted
    public Mono<ReactiveSingleQueryTransactionResult> query(String statement, SingleQueryTransactionConfig queryOptions) {
        return query(null, statement, queryOptions);
    }

    /**
     * Performs a single query transaction, with a scope context and default configuration.
     *
     * @param statement the statement to execute.
     * @param scope the query will be executed in the context of this scope, so it can refer to a collection on this scope
     *              rather than needed to provide the full keyspace.
     * @return a ReactiveSingleQueryTransactionResult
     */
    @Stability.Uncommitted
    public Mono<ReactiveSingleQueryTransactionResult> query(ReactiveScope scope, String statement) {
        return query(scope, statement, SingleQueryTransactionConfigBuilder.create().build());
    }

    /**
     * Performs a single query transaction, with a scope context and custom configuration.
     *
     * @param statement the statement to execute.
     * @param scope the query will be executed in the context of this scope, so it can refer to a collection on this scope
     *              rather than needed to provide the full keyspace.
     * @param queryOptions configuration options.
     * @return a ReactiveSingleQueryTransactionResult
     */
    @Stability.Uncommitted
    public Mono<ReactiveSingleQueryTransactionResult> query(ReactiveScope scope, String statement, SingleQueryTransactionConfig queryOptions) {
        return Mono.defer(() -> {
            AtomicReference<ReactiveQueryResult> queryResult = new AtomicReference<>();
            return run((ctx) -> ctx.query(scope, statement, queryOptions.queryOptions(), true)
                    .doOnNext(qr -> queryResult.set(qr))
                    .then(), queryOptions.convert())
                    .map(result -> new ReactiveSingleQueryTransactionResult(result.log(), queryResult.get()));
        });
    }

    @Stability.Internal
    @Deprecated // Prefer setting TransactionConfigBuilder#testFactories now
    public void setAttemptContextFactory(AttemptContextFactory factory) {
        this.attemptContextFactory = factory;
    }
    public AttemptContextReactive newAttemptContextReactive(){
        PerTransactionConfig perConfig = PerTransactionConfigBuilder.create().build();
        MergedTransactionConfig merged = new MergedTransactionConfig(config, Optional.of(perConfig));

        TransactionContext overall = new TransactionContext(
            cleanup().clusterData().cluster().environment().requestTracer(),
            cleanup().clusterData().cluster().environment().eventBus(),
            UUID.randomUUID().toString(), now(), Duration.ZERO, merged);

        String txnId = UUID.randomUUID().toString();
        overall.LOGGER.info(configDebug(config, perConfig));
        return createAttemptContext(overall, merged, txnId);
    }

}
