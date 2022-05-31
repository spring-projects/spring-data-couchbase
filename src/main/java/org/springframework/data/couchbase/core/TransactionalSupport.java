package org.springframework.data.couchbase.core;

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.function.Function;

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import org.springframework.lang.Nullable;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.ReactiveCollection;

@Stability.Internal
class TransactionalSupportHelper {
    public final CouchbaseDocument converted;
    public final Long cas;
    public final ReactiveCollection collection;
    public final @Nullable CoreTransactionAttemptContext ctx;

    public TransactionalSupportHelper(CouchbaseDocument doc, Long cas, ReactiveCollection collection,
                                      @Nullable CoreTransactionAttemptContext ctx) {
        this.converted = doc;
        this.cas = cas;
        this.collection = collection;
        this.ctx = ctx;
    }
}

/**
 * Checks if this operation is being run inside a transaction, and calls a non-transactional or transactional callback
 * as appropriate.
 */
@Stability.Internal
public class TransactionalSupport {
    public static <T> Mono<T> one(Mono<ReactiveCouchbaseTemplate> tmpl, CouchbaseTransactionalOperator transactionalOperator,
                                  String scopeName, String collectionName, ReactiveTemplateSupport support, T object,
                                  Function<TransactionalSupportHelper, Mono<T>> nonTransactional, Function<TransactionalSupportHelper, Mono<T>> transactional) {
        return tmpl.flatMap(template -> template.getCouchbaseClientFactory().withScope(scopeName)
                .getCollectionMono(collectionName).flatMap(collection -> support.encodeEntity(object)
                        .flatMap(converted -> tmpl.map(tp -> tp.getCouchbaseClientFactory().getResources()).flatMap(s -> {
                            TransactionalSupportHelper gsh = new TransactionalSupportHelper(converted, support.getCas(object),
                                    collection.reactive(), s.getCore() != null ? s.getCore()
                                    : (transactionalOperator != null ? transactionalOperator.getAttemptContext() : null));
                            if (gsh.ctx == null) {
                                System.err.println("non-tx");
                                return nonTransactional.apply(gsh);
                            } else {
                                System.err.println("tx");
                                return transactional.apply(gsh);
                            }
                        })).onErrorMap(throwable -> {
                            if (throwable instanceof RuntimeException) {
                                return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
                            } else {
                                return throwable;
                            }
                        })));
    }

    public static Mono<Void> verifyNotInTransaction(Mono<ReactiveCouchbaseTemplate> tmpl, String methodName) {
        return tmpl.flatMap(tp -> Mono.just(tp.getCouchbaseClientFactory().getResources())
                .flatMap(s -> {
                    if (s.hasActiveTransaction()) {
                        return Mono.error(new IllegalArgumentException(methodName + "can not be used inside a transaction"));
                    }
                    else {
                        return Mono.empty();
                    }
                }));
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
