package org.springframework.data.couchbase.core;

import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;

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

    ExecutableFindByIdSupport(CouchbaseTemplate template, Class<T> domainType, String collection, List<String> fields) {
      this.template = template;
      this.domainType = domainType;
      this.collection = collection;
      this.fields = fields;
    }

    @Override
    public T one(final String id) {
      try {
        GetOptions options = getOptions().transcoder(RawJsonTranscoder.INSTANCE);
        if (fields != null && !fields.isEmpty()) {
          options.project(fields);
        }
        GetResult result = template.getCollection(collection).get(id, options);
        return template.support().decodeEntity(id, result.contentAs(String.class), result.cas(), domainType);
      } catch (RuntimeException ex) {
        throw template.potentiallyConvertRuntimeException(ex);
      }
    }

    @Override
    public Collection<? extends T> all(final Collection<String> ids) {
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
        )
        .collectList()
        .block();
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

}
