package org.springframework.data.couchbase.core;

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import org.springframework.data.couchbase.transaction.CouchbaseResourceHolder;
import org.springframework.transaction.reactive.TransactionContext;
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
    public static Mono<Optional<CouchbaseResourceHolder>> checkForTransactionInThreadLocalStorage(@Nullable CouchbaseTransactionalOperator operator) {
        return Mono.deferContextual(ctx -> {
            if (operator != null) {
                // gp: this isn't strictly correct, as it won't preserve the result map correctly, but tbh want to remove CouchbaseTransactionalOperator anyway
                return Mono.just(Optional.of(new CouchbaseResourceHolder(operator.getAttemptContext())));
            }

            // need to make usage of TransactionSynchronizationManager consistent.
            // ReactiveTransactionWrapper stores CouchbaseResourceHolder directly as a TSM.resource
            // ReactiveCouchbaseTransactionManager (and by extension TransactionalOperator)
            CouchbaseResourceHolder blocking = (CouchbaseResourceHolder) TransactionSynchronizationManager.getResource(CouchbaseResourceHolder.class);
            // Stored in the TransactionContext (same as synchronizationManager does)
            TransactionContext tc = ctx.hasKey(TransactionContext.class) ? ctx.get(TransactionContext.class) : null;
            CouchbaseResourceHolder holder = tc != null ? (CouchbaseResourceHolder)tc.getResources().get(CouchbaseResourceHolder.class) : null;
            // Stored directly Reactive ctx
            if(holder == null ){
                holder =  ctx.hasKey(CouchbaseResourceHolder.class) ?  ctx.get(CouchbaseResourceHolder.class) : null;
            }
            Optional<CouchbaseResourceHolder> reactive = Optional.ofNullable(holder);//ctx.getOrEmpty(ReactiveCouchbaseResourceHolder.class);

            if (blocking != null && reactive.isPresent()) {
                throw new IllegalStateException("Both blocking and reactive transaction contexts are set simultaneously");
            }

            if (blocking != null) {
                printThrough("blocking core: ",blocking.getCore());
                return Mono.just(Optional.of(blocking));
            } else  if(reactive.isPresent()){
                printThrough("reactive core: ",reactive.get().getCore());
            } else {
                printThrough("no core:",null);
            }
            return Mono.just(reactive);
        });
    }

    private static <T> T printThrough(String label, T obj){
        System.err.println(label+obj);
        return obj;
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
