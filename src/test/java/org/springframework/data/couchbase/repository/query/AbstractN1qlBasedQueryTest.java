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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.couchbase.core.query.N1QLExpression.select;
import static org.springframework.data.couchbase.core.query.N1QLExpression.x;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import com.couchbase.client.java.json.JsonValue;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.core.query.WithConsistency;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import org.springframework.data.repository.query.ReturnedType;

/**
 * Unit tests for {@link AbstractN1qlBasedQuery}.
 *
 * @author Simon Basle
 * @author Subhashni Balakrishnan
 * @author Oliver Gierke
 */
public class AbstractN1qlBasedQueryTest {
	
  CouchbaseMappingContext context = new CouchbaseMappingContext();
  ProjectionFactory projectionFactory = new SpelAwareProxyProjectionFactory();
  RepositoryMetadata metadata = DefaultRepositoryMetadata.getMetadata(SampleRepository.class);

  @Test
  public void testEmptyArgumentsShouldProduceSimpleN1qlQuery() throws Exception {
    N1QLExpression st = select(x("*"));
    N1QLQuery query = AbstractN1qlBasedQuery.buildQuery(st, JsonArray.create(), QueryScanConsistency.NOT_BOUNDED);
    JsonObject queryObject = query.n1ql();
    assertEquals(st.toString(), query.getExpression());
    // TODO: some sort of equals in QueryOptions?
    //  assertEquals(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.NOT_BOUNDED).toString(), query.getOptions().toString());
    assertFalse(queryObject.containsKey("args"));
  }

  @Test
  public void testSimpleArgumentShouldProduceParametrizedQuery() throws Exception {
    N1QLExpression st = select(x("*"));
    List<Object> params = new ArrayList<Object>(2);
    params.add("test");
    JsonArray placeholderValues = JsonArray.from(params);
    N1QLQuery query = AbstractN1qlBasedQuery.buildQuery(st, placeholderValues, QueryScanConsistency.NOT_BOUNDED);
    JsonObject queryObject = query.n1ql();

    assertEquals(st.toString(), query.getExpression());
    // TODO: some sort of equals in QueryOptions?
    //  assertEquals(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.NOT_BOUNDED), query.getOptions());
    assertTrue(queryObject.containsKey("args"));
    JsonArray args = queryObject.getArray("args");
    assertEquals(1, args.size());
    assertEquals("test", args.get(0));
  }

  @Test
  public void testMultipleArgumentsShouldProduceParametrizedQuery() throws Exception {
    N1QLExpression st = select(x("*"));
    List<Object> params = new ArrayList<Object>(2);
    params.add(123L);
    params.add("test");
    JsonArray placeholderValues = JsonArray.from(params);
    N1QLQuery query = AbstractN1qlBasedQuery.buildQuery(st, placeholderValues, QueryScanConsistency.NOT_BOUNDED);
    JsonObject queryObject = query.n1ql();

    assertEquals(st.toString(), query.getExpression());
    // TODO: some sort of equals in QueryOptions?
    //  assertEquals(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.NOT_BOUNDED), query.getOptions());
    assertTrue(queryObject.containsKey("args"));
    JsonArray args = queryObject.getArray("args");
    assertEquals(2, args.size());
    assertEquals(123L, args.get(0));
    assertEquals("test", args.get(1));
  }

  @Test
  public void shouldChooseCollectionExecutionWhenCollectionType() throws Exception {
  	
    Method method = SampleRepository.class.getMethod("findAll");
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);

    N1QLQuery query = Mockito.mock(N1QLQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1QLQuery.class), any(N1QLQuery.class), any(QueryMethod.class), any(Pageable.class), any(Class.class)))
        .thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Object.class);
    verify(mock).executeCollection(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executeEntity(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executeStream(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executePaged(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSliced(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSingleProjection(any(N1QLQuery.class));
  }

  @Test
  public void shouldChooseEntityExecutionWhenEntityType() throws Exception {
    
    Method method = SampleRepository.class.getMethod("findById", Integer.class);
  	
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);

    N1QLQuery query = Mockito.mock(N1QLQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1QLQuery.class), any(N1QLQuery.class), any(QueryMethod.class), any(Pageable.class), any(Class.class)))
        .thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Sample.class);
    verify(mock, never()).executeCollection(any(N1QLQuery.class), any(Class.class));
    verify(mock).executeEntity(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executeStream(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executePaged(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSliced(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSingleProjection(any(N1QLQuery.class));
  }

  @Test
  public void shouldChooseStreamExecutionWhenStreamType() throws Exception {
  	
    Method method = SampleRepository.class.getMethod("streamAll");
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);

    N1QLQuery query = Mockito.mock(N1QLQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1QLQuery.class), any(N1QLQuery.class), any(QueryMethod.class), any(Pageable.class),
        any(Class.class)))
        .thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Sample.class);
    verify(mock, never()).executeCollection(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executeEntity(any(N1QLQuery.class), any(Class.class));
    verify(mock).executeStream(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executePaged(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSliced(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSingleProjection(any(N1QLQuery.class));
  }

  @Test
  public void shouldChoosePagedExecutionWhenPageType() throws Exception {
  	
    Method method = SampleRepository.class.getMethod("findAllPaged", Pageable.class);
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);

    N1QLQuery query = Mockito.mock(N1QLQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1QLQuery.class), any(N1QLQuery.class), any(QueryMethod.class), any(Pageable.class),
        any(Class.class)))
        .thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Sample.class);

    verify(mock, never()).executeCollection(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executeEntity(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executeStream(any(N1QLQuery.class), any(Class.class));
    verify(mock).executePaged(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSliced(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSingleProjection(any(N1QLQuery.class));
  }

  @Test
  public void shouldChooseSlicedExecutionWhenSliceType() throws Exception {

    Method method = SampleRepository.class.getMethod("findAllSliced", Pageable.class);
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);

    N1QLQuery query = Mockito.mock(N1QLQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1QLQuery.class), any(N1QLQuery.class), any(QueryMethod.class), any(Pageable.class),
        any(Class.class))).thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Sample.class);

    verify(mock, never()).executeCollection(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executeEntity(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executeStream(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executePaged(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock).executeSliced(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSingleProjection(any(N1QLQuery.class));
  }

  @Test
  public void shouldExecuteSingleProjectionWhenRandomObjectReturnType() throws Exception {

    Method method = SampleRepository.class.getMethod("countDown");
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);

    N1QLQuery query = Mockito.mock(N1QLQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1QLQuery.class), any(N1QLQuery.class), any(QueryMethod.class), any(Pageable.class),
        any(Class.class))).thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Sample.class);

    verify(mock, never()).executeCollection(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executeEntity(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executeStream(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executePaged(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSliced(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock).executeSingleProjection(any(N1QLQuery.class));
  }

  @Test
  public void shouldExecuteSingleProjectionWhenPrimitiveReturnType() throws Exception {
  	
    Method method = SampleRepository.class.getMethod("longMethod");
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);

    N1QLQuery query = Mockito.mock(N1QLQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1QLQuery.class), any(N1QLQuery.class), any(QueryMethod.class), any(Pageable.class),
        any(Class.class))).thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Sample.class);

    verify(mock, never()).executeCollection(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executeEntity(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executeStream(any(N1QLQuery.class), any(Class.class));
    verify(mock, never()).executePaged(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSliced(any(N1QLQuery.class), any(N1QLQuery.class), any(Pageable.class), any(Class.class));
    verify(mock).executeSingleProjection(any(N1QLQuery.class));
  }

  @Test // DATACOUCH-206
  public void shouldPickConsistencyFromAnnotation() throws NoSuchMethodException {
    Class<SampleRepository> repositoryClass = SampleRepository.class;

    CouchbaseQueryMethod defaultQueryMethod = new CouchbaseQueryMethod(repositoryClass.getMethod("findAll"),
                                                                       metadata,
                                                                       projectionFactory,
                                                                       context);


    CouchbaseQueryMethod unboundedQueryMethod = new CouchbaseQueryMethod(repositoryClass.getMethod("streamAll"),
                                                                         metadata,
                                                                         projectionFactory,
                                                                         context);

    CouchbaseTemplate template = mock(CouchbaseTemplate.class);
    when(template.getDefaultConsistency()).thenReturn(Consistency.STRONGLY_CONSISTENT);

    QueryScanConsistency defaultConsistency = new SampleQuery(defaultQueryMethod, template).getScanConsistency();
    assertEquals(defaultConsistency, Consistency.STRONGLY_CONSISTENT.n1qlConsistency());

    QueryScanConsistency unboundedConsistency = new SampleQuery(unboundedQueryMethod, template).getScanConsistency();
    assertEquals(unboundedConsistency, QueryScanConsistency.NOT_BOUNDED);

  }

  static class Sample {
  		Integer id;
  }

  interface SampleRepository extends Repository<Sample, Integer> {

    Collection<Sample> findAll();

    Sample findById(Integer id);

    @WithConsistency(QueryScanConsistency.NOT_BOUNDED)
    Stream<Sample> streamAll();

    Page<Sample> findAllPaged(Pageable pageable);

    Slice<Sample> findAllSliced(Pageable pageable);

    void modifyingMethod();

    CountDownLatch countDown();

    long longMethod();
  }

  class SampleQuery extends AbstractN1qlBasedQuery {

    protected SampleQuery(CouchbaseQueryMethod queryMethod,
                          CouchbaseOperations couchbaseOperations) {
      super(queryMethod, couchbaseOperations);
    }

    @Override
    protected N1QLExpression getCount(ParameterAccessor accessor, Object[] runtimeParameters) {
      return null;
    }

    @Override
    protected boolean useGeneratedCountQuery() {
      return false;
    }

    @Override
    protected N1QLExpression getExpression(ParameterAccessor accessor, Object[] runtimeParameters, ReturnedType returnedType) {
      return null;
    }

    @Override
    protected JsonValue getPlaceholderValues(ParameterAccessor accessor) {
      return null;
    }
  }
}
