package org.springframework.data.couchbase.core;

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.Function;

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import org.springframework.lang.Nullable;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.ReactiveCollection;

// todo gp better name
@Stability.Internal
class GenericSupportHelper {
    public final CouchbaseDocument converted;
    public final Long cas;
    public final ReactiveCollection collection;
    public final @Nullable CoreTransactionAttemptContext ctx;

    public GenericSupportHelper(CouchbaseDocument doc, Long cas, ReactiveCollection collection,
                                @Nullable CoreTransactionAttemptContext ctx) {
        this.converted = doc;
        this.cas = cas;
        this.collection = collection;
        this.ctx = ctx;
    }
}

// todo gp better name
@Stability.Internal
public class GenericSupport {
    public static <T> Mono<T> one(Mono<ReactiveCouchbaseTemplate> tmpl, CouchbaseTransactionalOperator transactionalOperator,
                                  String scopeName, String collectionName, ReactiveTemplateSupport support, T object,
                                  Function<GenericSupportHelper, Mono<T>> nonTransactional, Function<GenericSupportHelper, Mono<T>> transactional) {
        return tmpl.flatMap(template -> template.getCouchbaseClientFactory().withScope(scopeName)
                .getCollection(collectionName).flatMap(collection -> support.encodeEntity(object)
                        .flatMap(converted -> tmpl.flatMap(tp -> tp.getCouchbaseClientFactory().getTransactionResources(null).flatMap(s -> {
                            GenericSupportHelper gsh = new GenericSupportHelper(converted, support.getCas(object),
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
                        }))));
    }

}
