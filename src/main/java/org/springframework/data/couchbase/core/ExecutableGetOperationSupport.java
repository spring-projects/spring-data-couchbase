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

public class ExecutableGetOperationSupport implements ExecutableGetOperation {

  private final CouchbaseTemplate template;

  ExecutableGetOperationSupport(CouchbaseTemplate template) {
    this.template = template;
  }

  @Override
  public <T> ExecutableGet<T> get(Class<T> domainType) {
    return new ExecutableGetSupport<>(template, domainType, null, null);
  }

  static class ExecutableGetSupport<T> implements ExecutableGet<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final String collection;
    private final List<String> fields;

    ExecutableGetSupport(CouchbaseTemplate template, Class<T> domainType, String collection, List<String> fields) {
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
        )
        .collectList()
        .block();
    }

    @Override
    public TerminatingGet<T> inCollection(final String collection) {
      Assert.hasText(collection, "Collection must not be null nor empty.");
      return new ExecutableGetSupport<>(template, domainType, collection, fields);
    }

    @Override
    public GetWithCollection<T> project(String... fields) {
      Assert.notEmpty(fields, "Fields must not be null nor empty.");
      return new ExecutableGetSupport<>(template, domainType, collection, Arrays.asList(fields));
    }
  }

}
