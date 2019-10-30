/*
 * Copyright 2015-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.query;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.couchbase.client.java.json.JsonValue;
import com.couchbase.client.java.query.QueryScanConsistency;

import org.junit.*;
import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
import org.springframework.data.couchbase.core.RxJavaCouchbaseTemplate;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.WithConsistency;
import org.springframework.data.couchbase.repository.ReactiveCouchbaseRepository;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ReturnedType;
import reactor.core.publisher.Flux;

/**
 * Unit tests for {@link ReactiveAbstractN1qlBasedQuery}.
 *
 * @author Johannes Jasper
 */
public class ReactiveAbstractN1qlBasedQueryTest {
	
  CouchbaseMappingContext context = new CouchbaseMappingContext();
  ProjectionFactory projectionFactory = new SpelAwareProxyProjectionFactory();
  RepositoryMetadata metadata = DefaultRepositoryMetadata.getMetadata(SampleRepository.class);


  @Test // DATACOUCH-206
  public void shouldPickConsistencyFromAnnotation() throws NoSuchMethodException {
    Class<SampleRepository> repositoryClass = SampleRepository.class;

    CouchbaseQueryMethod defaultQueryMethod = new CouchbaseQueryMethod(repositoryClass.getMethod("findAll"),
                                                                       metadata,
                                                                       projectionFactory,
                                                                       context);


    CouchbaseQueryMethod unboundedQueryMethod = new CouchbaseQueryMethod(repositoryClass.getMethod("findByName"),
                                                                         metadata,
                                                                         projectionFactory,
                                                                         context);

    RxJavaCouchbaseTemplate template = mock(RxJavaCouchbaseTemplate.class);
    when(template.getDefaultConsistency()).thenReturn(Consistency.STRONGLY_CONSISTENT);

    QueryScanConsistency defaultConsistency = new SampleQuery(defaultQueryMethod, template).getScanConsistency();
    assertEquals(defaultConsistency, Consistency.STRONGLY_CONSISTENT.n1qlConsistency());

    QueryScanConsistency unboundedConsistency = new SampleQuery(unboundedQueryMethod, template).getScanConsistency();
    assertEquals(unboundedConsistency, QueryScanConsistency.NOT_BOUNDED);

  }

  static class Sample {
  		Integer name;
  }

  interface SampleRepository extends ReactiveCouchbaseRepository<Sample, Integer> {

    Flux<Sample> findAll();

    @WithConsistency(QueryScanConsistency.NOT_BOUNDED)
    Flux<Sample> findByName();
  }

  class SampleQuery extends ReactiveAbstractN1qlBasedQuery {

    protected SampleQuery(CouchbaseQueryMethod queryMethod,
                          RxJavaCouchbaseOperations couchbaseOperations) {
      super(queryMethod, couchbaseOperations);
    }

    @Override
    protected N1QLExpression getExpression(ParameterAccessor accessor,
                                          Object[] runtimeParameters,
                                          ReturnedType returnedType) {
      return null;
    }

    @Override
    protected JsonValue getPlaceholderValues(ParameterAccessor accessor) {
      return null;
    }
  }
}
