package org.springframework.data.couchbase.core;

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.threadlocal.TransactionMarkerOwner;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.transaction.CouchbaseResourceHolder;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionContext;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.ClassUtils;
import reactor.core.publisher.Mono;

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
		public static Mono<Optional<CouchbaseResourceHolder>> checkForTransactionInThreadLocalStorage() {
			return TransactionMarkerOwner.get().flatMap(markerOpt -> {
				Optional<CouchbaseResourceHolder> out = markerOpt
						.flatMap(marker -> Optional.of(new CouchbaseResourceHolder(marker.context())));
				return Mono.just(out);
			});
    }

    public static Mono<Void> verifyNotInTransaction(String methodName) {
        return checkForTransactionInThreadLocalStorage()
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
            TransactionOperationFailedException err = TransactionOperationFailedException.Builder.createError()
                    .retryTransaction()
                    .cause(new CasMismatchException(null))
                    .build();
            return ctx.operationFailed(err);
        } catch (Throwable err) {
            return new RuntimeException(err);
        }

    }
}
