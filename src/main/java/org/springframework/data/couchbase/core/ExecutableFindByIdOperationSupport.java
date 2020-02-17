package org.springframework.data.couchbase.core;

import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.GetOptions;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.couchbase.client.java.kv.GetOptions.getOptions;

public class ExecutableFindByIdOperationSupport implements ExecutableFindByIdOperation {

  private final CouchbaseTemplate template;

  ExecutableFindByIdOperationSupport(CouchbaseTemplate template) {
    this.template = template;
  }

  @Override
  public <T> ExecutableFindById<T> findById(Class<T> domainType) {
    return new ExecutableFindByIdSupport<>(template, domainType, null, null);
  }

  static class ExecutableFindByIdSupport<T> implements ExecutableFindById<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final String collection;
    private final List<String> fields;
    private final TerminatingReactiveFindByIdSupport<T> reactiveSupport;

    ExecutableFindByIdSupport(CouchbaseTemplate template, Class<T> domainType, String collection, List<String> fields) {
      this.template = template;
      this.domainType = domainType;
      this.collection = collection;
      this.fields = fields;
      this.reactiveSupport = new TerminatingReactiveFindByIdSupport<>(template, domainType, collection, fields);
    }

    @Override
    public T one(final String id) {
      return reactiveSupport.one(id).block();
    }

    @Override
    public Collection<? extends T> all(final Collection<String> ids) {
      return reactiveSupport.all(ids).collectList().block();
    }

    @Override
    public TerminatingReactiveFindById<T> reactive() {
      return null;
    }

    @Override
    public TerminatingFindById<T> inCollection(final String collection) {
      Assert.hasText(collection, "Collection must not be null nor empty.");
      return new ExecutableFindByIdSupport<>(template, domainType, collection, fields);
    }

    @Override
    public FindByIdWithCollection<T> project(String... fields) {
      Assert.notEmpty(fields, "Fields must not be null nor empty.");
      return new ExecutableFindByIdSupport<>(template, domainType, collection, Arrays.asList(fields));
    }
  }

  static class TerminatingReactiveFindByIdSupport<T> implements TerminatingReactiveFindById<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final String collection;
    private final List<String> fields;

    TerminatingReactiveFindByIdSupport(CouchbaseTemplate template, Class<T> domainType, String collection, List<String> fields) {
      this.template = template;
      this.domainType = domainType;
      this.collection = collection;
      this.fields = fields;
    }

    @Override
    public Mono<T> one(final String id) {
      return Mono
        .just(id)
        .flatMap(docId -> {
          GetOptions options = getOptions().transcoder(RawJsonTranscoder.INSTANCE);
          if (fields != null && !fields.isEmpty()) {
            options.project(fields);
          }
          return template.getCollection(collection).reactive().get(docId, options);
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
    public Flux<? extends T> all(final Collection<String> ids) {
      return Flux
        .fromIterable(ids)
        .flatMap(id -> template.getCollection(collection).reactive()
          .get(id, getOptions().transcoder(RawJsonTranscoder.INSTANCE))
          .map(result -> template.support().decodeEntity(id, result.contentAs(String.class), result.cas(), domainType))
          .onErrorMap(throwable -> {
            if (throwable instanceof RuntimeException) {
              return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
            } else {
              return throwable;
            }
          })
        );
    }
  }

}
