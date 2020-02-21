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

import com.couchbase.client.java.analytics.ReactiveAnalyticsResult;
import com.couchbase.client.java.query.ReactiveQueryResult;
import org.springframework.data.couchbase.core.query.AnalyticsQuery;
import org.springframework.data.couchbase.core.query.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Stream;

public class ExecutableFindByAnalyticsOperationSupport implements ExecutableFindByAnalyticsOperation {

  private static final AnalyticsQuery ALL_QUERY = new AnalyticsQuery();

  private final CouchbaseTemplate template;

  public ExecutableFindByAnalyticsOperationSupport(final CouchbaseTemplate template) {
    this.template = template;
  }

  @Override
  public <T> ExecutableFindByAnalytics<T> findByAnalytics(final Class<T> domainType) {
    return new ExecutableFindByAnalyticsSupport<>(template, domainType, ALL_QUERY);
  }

  static class ExecutableFindByAnalyticsSupport<T> implements ExecutableFindByAnalytics<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final TerminatingReactiveFindByAnalytics<T> reactiveSupport;

    ExecutableFindByAnalyticsSupport(final CouchbaseTemplate template, final Class<T> domainType, final AnalyticsQuery query) {
      this.template = template;
      this.domainType = domainType;
      this.reactiveSupport = new TerminatingReactiveFindByAnalyticsSupport<>(template, domainType, query);
    }

    @Override
    public T oneValue() {
      return reactiveSupport.one().block();
    }

    @Override
    public T firstValue() {
      return reactiveSupport.first().block();
    }

    @Override
    public List<T> all() {
      return reactiveSupport.all().collectList().block();
    }

    @Override
    public TerminatingFindByAnalytics<T> matching(final AnalyticsQuery query) {
      return new ExecutableFindByAnalyticsSupport<>(template, domainType, query);
    }

    @Override
    public Stream<T> stream() {
      return reactiveSupport.all().toStream();
    }

    @Override
    public long count() {
      return reactiveSupport.count().block();
    }

    @Override
    public boolean exists() {
      return count() > 0;
    }

    @Override
    public TerminatingReactiveFindByAnalytics<T> reactive() {
      return reactiveSupport;
    }
  }

  static class TerminatingReactiveFindByAnalyticsSupport<T> implements TerminatingReactiveFindByAnalytics<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final AnalyticsQuery query;

    TerminatingReactiveFindByAnalyticsSupport(final CouchbaseTemplate template, final Class<T> domainType,
                                          final AnalyticsQuery query) {
      this.template = template;
      this.domainType = domainType;
      this.query = query;
    }

    @Override
    public Mono<T> one() {
      return all().single();
    }

    @Override
    public Mono<T> first() {
      return all().next();
    }

    @Override
    public Flux<T> all() {
      return Flux.defer(() -> {
        String statement = assembleEntityQuery(false);
        return template
          .getCouchbaseClientFactory()
          .getCluster()
          .reactive()
          .analyticsQuery(statement)
          .onErrorMap(throwable -> {
            if (throwable instanceof RuntimeException) {
              return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
            } else {
              return throwable;
            }
          })
          .flatMapMany(ReactiveAnalyticsResult::rowsAsObject)
          .map(row -> {
            String id = row.getString("__id");
            long cas = row.getLong("__cas");
            row.removeKey("__id");
            row.removeKey("__cas");
            return template.support().decodeEntity(id, row.toString(), cas, domainType);
          });
      });
    }

    @Override
    public Mono<Long> count() {
      return Mono.defer(() -> {
        String statement = assembleEntityQuery(true);
        return template
          .getCouchbaseClientFactory()
          .getCluster()
          .reactive()
          .analyticsQuery(statement)
          .onErrorMap(throwable -> {
            if (throwable instanceof RuntimeException) {
              return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
            } else {
              return throwable;
            }
          })
          .flatMapMany(ReactiveAnalyticsResult::rowsAsObject)
          .map(row -> row.getLong("__count"))
          .next();
      });
    }

    @Override
    public Mono<Boolean> exists() {
      return count().map(count -> count > 0);
    }

    private String assembleEntityQuery(final boolean count) {
      final String bucket = "`" + template.getBucketName() + "`";

      final StringBuilder statement = new StringBuilder("SELECT ");
      if (count) {
        statement.append("count(*) as __count");
      } else {
        statement.append("meta().id as __id, meta().cas as __cas, ").append(bucket).append(".*");
      }

      final String dataset = template.support().getJavaNameForEntity(domainType);
      statement.append(" FROM ").append(dataset);

      query.appendSort(statement);
      query.appendSkipAndLimit(statement);
      return statement.toString();
    }

  }

}
