package org.springframework.data.couchbase.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.Function;

// todo gp better name
@Stability.Internal
class GenericSupportHelper {
    public final CouchbaseDocument converted;
    public final Collection collection;
    public final @Nullable TransactionAttemptContext ctx;

    public GenericSupportHelper(CouchbaseDocument doc, Collection collection, @Nullable TransactionAttemptContext ctx) {
        this.converted = doc;
        this.collection = collection;
        this.ctx = ctx;
    }
}

// todo gp better name
@Stability.Internal
public class GenericSupport {
    public static <T> Mono<T> one(ReactiveCouchbaseTemplate template,
                                  String scopeName,
                                  String collectionName,
                                  ReactiveTemplateSupport support,
                                  T object,
                                  Function<GenericSupportHelper, Mono<T>> nonTransactional,
                                  Function<GenericSupportHelper, Mono<T>> transactional) {
        // todo gp how safe is this?  I think we can switch threads potentially
        Optional<TransactionAttemptContext> ctxr = Optional.ofNullable((TransactionAttemptContext)
                org.springframework.transaction.support.TransactionSynchronizationManager.getResource(TransactionAttemptContext.class));

        return template.getCouchbaseClientFactory().withScope(scopeName).getCollection(collectionName)
                .flatMap(collection ->
                        support.encodeEntity(object)
                                .flatMap(converted -> {
                                    GenericSupportHelper gsh = new GenericSupportHelper(converted, collection, ctxr.orElse(null));
                                    if (!ctxr.isPresent()) {
                                        return nonTransactional.apply(gsh);
                                    } else {
                                        return transactional.apply(gsh);
                                    }
                                }))
                .onErrorMap(throwable -> {
                    if (throwable instanceof RuntimeException) {
                        return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
                    } else {
                        return throwable;
                    }
                });
    }

}
