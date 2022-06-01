package org.springframework.data.couchbase.core;

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseResourceHolder;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.Optional;

import org.springframework.lang.Nullable;

import com.couchbase.client.core.annotation.Stability;

@Stability.Internal
public class TransactionalSupport {

    /**
     * Returns non-empty iff in a transaction.  It determines this from thread-local storage and/or reactive context.
     * <p>
     * The user could be doing a reactive operation (with .block()) inside a blocking transaction (like @Transactional).
     * Or a blocking operation inside a ReactiveTransactionsWrapper transaction (which would be a bad idea).
     * So, need to check both thread-local storage and reactive context.
     */
    public static Mono<Optional<ReactiveCouchbaseResourceHolder>> checkForTransactionInThreadLocalStorage(@Nullable CouchbaseTransactionalOperator operator) {
        return Mono.deferContextual(ctx -> {
            if (operator != null) {
                // gp: this isn't strictly correct, as it won't preserve the result map correctly, but tbh want to remove CouchbaseTransactionalOperator anyway
                return Mono.just(Optional.of(new ReactiveCouchbaseResourceHolder(operator.getAttemptContext())));
            }

            ReactiveCouchbaseResourceHolder blocking = (ReactiveCouchbaseResourceHolder) TransactionSynchronizationManager.getResource(ReactiveCouchbaseResourceHolder.class);
            Optional<ReactiveCouchbaseResourceHolder> reactive = ctx.getOrEmpty(ReactiveCouchbaseResourceHolder.class);

            if (blocking != null && reactive.isPresent()) {
                throw new IllegalStateException("Both blocking and reactive transaction contexts are set simultaneously");
            }

            if (blocking != null) {
                return Mono.just(Optional.of(blocking));
            }

            return Mono.just(reactive);
        });
    }

    public static Mono<Void> verifyNotInTransaction(String methodName) {
        return checkForTransactionInThreadLocalStorage(null)
                .flatMap(s -> {
                    if (s.isPresent()) {
                        return Mono.error(new IllegalArgumentException(methodName + "can not be used inside a transaction"));
                    }
                    else {
                        return Mono.empty();
                    }
                });
    }

    public static RuntimeException retryTransactionOnCasMismatch(CoreTransactionAttemptContext ctx, long cas1, long cas2) {
        try {
            ctx.logger().info(ctx.attemptId(), "Spring CAS mismatch %s != %s, retrying transaction", cas1, cas2);

            // todo gpx expose this in SDK
            Method method = CoreTransactionAttemptContext.class.getDeclaredMethod("operationFailed", TransactionOperationFailedException.class);
            method.setAccessible(true);
            TransactionOperationFailedException err = TransactionOperationFailedException.Builder.createError()
                    .retryTransaction()
                    .cause(new CasMismatchException(null))
                    .build();
            method.invoke(ctx, err);
            return err;
        } catch (Throwable err) {
            return new RuntimeException(err);
        }

    }
}
