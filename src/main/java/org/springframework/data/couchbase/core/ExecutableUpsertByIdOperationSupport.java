/*
 * Copyright 2012-2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.manager.user.UpsertUserOptions;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public class ExecutableUpsertByIdOperationSupport implements ExecutableUpsertByIdOperation {

  private final CouchbaseTemplate template;

  public ExecutableUpsertByIdOperationSupport(final CouchbaseTemplate template) {
    this.template = template;
  }

  @Override
  public <T> ExecutableUpsertById<T> upsertById(final Class<T> domainType) {
    Assert.notNull(domainType, "DomainType must not be null!");
    return new ExecutableUpsertByIdSupport<>(template, domainType, null, PersistTo.NONE, ReplicateTo.NONE,
      DurabilityLevel.NONE);
  }

  static class ExecutableUpsertByIdSupport<T> implements ExecutableUpsertById<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final String collection;
    private final PersistTo persistTo;
    private final ReplicateTo replicateTo;
    private final DurabilityLevel durabilityLevel;
    private final TerminatingReactiveUpsertByIdSupport<T> reactiveSupport;

    ExecutableUpsertByIdSupport(final CouchbaseTemplate template, final Class<T> domainType,
                                final String collection, final PersistTo persistTo, final ReplicateTo replicateTo,
                                final DurabilityLevel durabilityLevel) {
      this.template = template;
      this.domainType = domainType;
      this.collection = collection;
      this.persistTo = persistTo;
      this.replicateTo = replicateTo;
      this.durabilityLevel = durabilityLevel;
      this.reactiveSupport = new TerminatingReactiveUpsertByIdSupport<>(template, domainType, collection, persistTo,
        replicateTo, durabilityLevel);
    }

    @Override
    public T one(final T object) {
      return reactiveSupport.one(object).block();
    }

    @Override
    public Collection<? extends T> all(Collection<? extends T> objects) {
      return reactiveSupport.all(objects).collectList().block();
    }

    @Override
    public TerminatingReactiveUpsertById<T> reactive() {
      return reactiveSupport;
    }

    @Override
    public TerminatingUpsertById<T> inCollection(final String collection) {
      Assert.hasText(collection, "Collection must not be null nor empty.");
      return new ExecutableUpsertByIdSupport<>(template, domainType, collection, persistTo, replicateTo, durabilityLevel);
    }

    @Override
    public UpsertByIdWithCollection<T> withDurability(final DurabilityLevel durabilityLevel) {
      Assert.notNull(durabilityLevel, "Durability Level must not be null.");
      return new ExecutableUpsertByIdSupport<>(template, domainType, collection, persistTo, replicateTo, durabilityLevel);
    }

    @Override
    public UpsertByIdWithCollection<T> withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
      Assert.notNull(persistTo, "PersistTo must not be null.");
      Assert.notNull(replicateTo, "ReplicateTo must not be null.");
      return new ExecutableUpsertByIdSupport<>(template, domainType, collection, persistTo, replicateTo, durabilityLevel);
    }

  }

  static class TerminatingReactiveUpsertByIdSupport<T> implements TerminatingReactiveUpsertById<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final String collection;
    private final PersistTo persistTo;
    private final ReplicateTo replicateTo;
    private final DurabilityLevel durabilityLevel;

    TerminatingReactiveUpsertByIdSupport(final CouchbaseTemplate template, final Class<T> domainType,
                                final String collection, final PersistTo persistTo, final ReplicateTo replicateTo,
                                final DurabilityLevel durabilityLevel) {
      this.template = template;
      this.domainType = domainType;
      this.collection = collection;
      this.persistTo = persistTo;
      this.replicateTo = replicateTo;
      this.durabilityLevel = durabilityLevel;
    }

    @Override
    public Mono<T> one(T object) {
      return Mono.just(object).flatMap(o -> {
        CouchbaseDocument converted = template.support().encodeEntity(o);
        return template
          .getCollection(collection)
          .reactive()
          .upsert(converted.getId(), converted.getPayload(), buildUpsertOptions())
          .map(result -> {
            template.support().applyUpdatedCas(object, result.cas());
            return o;
          });
      }).onErrorMap(throwable -> {
        if (throwable instanceof RuntimeException) {
          return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
        } else {
          return throwable;
        }
      });
    }

    @Override
    public Flux<? extends T> all(Collection<? extends T> objects) {
      return Flux.fromIterable(objects).flatMap(this::one);
    }

    private UpsertOptions buildUpsertOptions() {
      final UpsertOptions options = UpsertOptions.upsertOptions();
      if (persistTo != PersistTo.NONE || replicateTo != ReplicateTo.NONE) {
        options.durability(persistTo, replicateTo);
      } else if (durabilityLevel != DurabilityLevel.NONE) {
        options.durability(durabilityLevel);
      }
      return options;
    }
  }

}
