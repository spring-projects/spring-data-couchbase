package org.springframework.data.couchbase.core;

import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.GetAnyReplicaOptions;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

import static com.couchbase.client.java.kv.GetAnyReplicaOptions.getAnyReplicaOptions;

public class ExecutableFindFromReplicasByIdOperationSupport implements ExecutableFindFromReplicasByIdOperation {

  private final CouchbaseTemplate template;

  ExecutableFindFromReplicasByIdOperationSupport(CouchbaseTemplate template) {
    this.template = template;
  }

  @Override
  public <T> ExecutableFindFromReplicasById<T> findFromReplicasById(Class<T> domainType) {
    return new ExecutableFindFromReplicasByIdSupport<>(template, domainType, null);
  }

  static class ExecutableFindFromReplicasByIdSupport<T> implements ExecutableFindFromReplicasById<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final String collection;
    private final TerminatingReactiveFindFromReplicasByIdSupport<T> reactiveSupport;

    ExecutableFindFromReplicasByIdSupport(CouchbaseTemplate template, Class<T> domainType, String collection) {
      this.template = template;
      this.domainType = domainType;
      this.collection = collection;
      this.reactiveSupport = new TerminatingReactiveFindFromReplicasByIdSupport<>(template, domainType, collection);
    }

    @Override
    public T any(String id) {
      return reactiveSupport.any(id).block();
    }

    @Override
    public Collection<? extends T> any(Collection<String> ids) {
      return reactiveSupport.any(ids).collectList().block();
    }

    @Override
    public TerminatingReactiveFindFromReplicasById<T> reactive() {
      return null;
    }

    @Override
    public TerminatingFindFromReplicasById<T> inCollection(final String collection) {
      Assert.hasText(collection, "Collection must not be null nor empty.");
      return new ExecutableFindFromReplicasByIdSupport<>(template, domainType, collection);
    }

  }

  static class TerminatingReactiveFindFromReplicasByIdSupport<T> implements TerminatingReactiveFindFromReplicasById<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final String collection;

    TerminatingReactiveFindFromReplicasByIdSupport(CouchbaseTemplate template, Class<T> domainType, String collection) {
      this.template = template;
      this.domainType = domainType;
      this.collection = collection;
    }

    @Override
    public Mono<T> any(final String id) {
      return Mono
        .just(id)
        .flatMap(docId -> {
          GetAnyReplicaOptions options = getAnyReplicaOptions().transcoder(RawJsonTranscoder.INSTANCE);
          return template.getCollection(collection).reactive().getAnyReplica(docId, options);
        })
        .map(result -> template.support().decodeEntity(id, result.contentAs(String.class), result.cas(), domainType))
        .onErrorMap(throwable -> {
          if (throwable instanceof RuntimeException) {
            return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
          } else {
            return throwable;
          }
        });
    }

    @Override
    public Flux<? extends T> any(Collection<String> ids) {
      return Flux.fromIterable(ids).flatMap(this::any);
    }

  }

}
