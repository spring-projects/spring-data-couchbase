package org.springframework.data.couchbase.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
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
    public final @Nullable CoreTransactionAttemptContext ctx;

    public GenericSupportHelper(CouchbaseDocument doc, Collection collection, @Nullable CoreTransactionAttemptContext ctx) {
        this.converted = doc;
        this.collection = collection;
        this.ctx = ctx;
    }

    CollectionIdentifier toCollectionIdentifier() {
        return new CollectionIdentifier(collection.bucketName(), Optional.of(collection.scopeName()), Optional.of(collection.name()));
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
        // todo gpx how safe is this?  I think we can switch threads potentially
//        Optional<TransactionAttemptContext> ctxr = Optional.ofNullable((TransactionAttemptContext)
//                org.springframework.transaction.support.TransactionSynchronizationManager.getResource(TransactionAttemptContext.class));
        Optional<CoreTransactionAttemptContext> ctxr = Optional.ofNullable((CoreTransactionAttemptContext)
                org.springframework.transaction.support.TransactionSynchronizationManager.getResource(CoreTransactionAttemptContext.class));

        return template.getCouchbaseClientFactory().withScope(scopeName).getCollection(collectionName)
                .flatMap(collection ->
                        support.encodeEntity(object)
                                .flatMap(converted -> {
                                    GenericSupportHelper gsh = new GenericSupportHelper(converted, collection, ctxr.orElse(null));
                                    if (!ctxr.isPresent()) {
                                        return nonTransactional.apply(gsh);
                                    } else {
//                                        System.out.println("Using ctx %s", ctxr.get());
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
