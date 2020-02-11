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
import com.couchbase.client.java.kv.ReplicateTo;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;

import java.util.Collection;

public class ExecutableUpsertOperationSupport implements ExecutableUpsertOperation {

  private final CouchbaseTemplate template;

  public ExecutableUpsertOperationSupport(final CouchbaseTemplate template) {
    this.template = template;
  }

  @Override
  public <T> ExecutableUpsert<T> upsert(final Class<T> domainType) {
    Assert.notNull(domainType, "DomainType must not be null!");
    return new ExecutableUpsertSupport<>(template, domainType, null, PersistTo.NONE, ReplicateTo.NONE,
      DurabilityLevel.NONE);
  }

  static class ExecutableUpsertSupport<T> implements ExecutableUpsert<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final String collection;
    private final PersistTo persistTo;
    private final ReplicateTo replicateTo;
    private final DurabilityLevel durabilityLevel;

    ExecutableUpsertSupport(final CouchbaseTemplate template, final Class<T> domainType,
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
    public T one(final T object) {
      try {
        CouchbaseDocument converted = template.support().encodeEntity(object);
        MutationResult result = template.getCollection(collection).upsert(converted.getId(), converted.getPayload());
        template.support().applyUpdatedCas(object, result.cas());
        return object;
      } catch (RuntimeException ex) {
        throw template.potentiallyConvertRuntimeException(ex);
      }
    }

    @Override
    public Collection<? extends T> all(Collection<? extends T> objects) {
      return Flux.fromIterable(objects).flatMap(entity -> {
        CouchbaseDocument converted = template.support().encodeEntity(entity);
        return template
          .getCollection(collection)
          .reactive()
          .upsert(converted.getId(), converted.getPayload())
          .map(res -> {
            template.support().applyUpdatedCas(entity, res.cas());
            return entity;
          }).onErrorMap(throwable -> {
            if (throwable instanceof RuntimeException) {
              return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
            } else {
              return throwable;
            }
          });
      }).collectList().block();
    }

    @Override
    public TerminatingUpsert<T> inCollection(final String collection) {
      Assert.hasText(collection, "Collection must not be null nor empty.");
      return new ExecutableUpsertSupport<>(template, domainType, collection, persistTo, replicateTo, durabilityLevel);
    }

    @Override
    public UpsertWithCollection<T> withDurability(final DurabilityLevel durabilityLevel) {
      Assert.notNull(durabilityLevel, "Durability Level must not be null.");
      return new ExecutableUpsertSupport<>(template, domainType, collection, persistTo, replicateTo, durabilityLevel);
    }

    @Override
    public UpsertWithCollection<T> withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
      Assert.notNull(persistTo, "PersistTo must not be null.");
      Assert.notNull(replicateTo, "ReplicateTo must not be null.");
      return new ExecutableUpsertSupport<>(template, domainType, collection, persistTo, replicateTo, durabilityLevel);
    }

  }

}
