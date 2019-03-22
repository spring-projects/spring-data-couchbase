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

import static com.couchbase.client.java.query.Select.select;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.ParameterizedN1qlQuery;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.SimpleN1qlQuery;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;

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
    Statement st = select("*");
    N1qlQuery query = AbstractN1qlBasedQuery.buildQuery(st, JsonArray.empty(), ScanConsistency.NOT_BOUNDED);
    JsonObject queryObject = query.n1ql();

    assertTrue(query instanceof SimpleN1qlQuery);
    assertEquals(st.toString(), query.statement().toString());
    assertEquals(N1qlParams.build().consistency(ScanConsistency.NOT_BOUNDED), query.params());
    assertFalse(queryObject.containsKey("args"));
  }

  @Test
  public void testSimpleArgumentShouldProduceParametrizedQuery() throws Exception {
    Statement st = select("*");
    List<Object> params = new ArrayList<Object>(2);
    params.add("test");
    JsonArray placeholderValues = JsonArray.from(params);
    N1qlQuery query = AbstractN1qlBasedQuery.buildQuery(st, placeholderValues, ScanConsistency.NOT_BOUNDED);
    JsonObject queryObject = query.n1ql();

    assertTrue(query instanceof ParameterizedN1qlQuery);
    assertEquals(st.toString(), query.statement().toString());
    assertEquals(N1qlParams.build().consistency(ScanConsistency.NOT_BOUNDED), query.params());
    assertTrue(queryObject.containsKey("args"));
    JsonArray args = queryObject.getArray("args");
    assertEquals(1, args.size());
    assertEquals("test", args.get(0));
  }

  @Test
  public void testMultipleArgumentsShouldProduceParametrizedQuery() throws Exception {
    Statement st = select("*");
    List<Object> params = new ArrayList<Object>(2);
    params.add(123L);
    params.add("test");
    JsonArray placeholderValues = JsonArray.from(params);
    N1qlQuery query = AbstractN1qlBasedQuery.buildQuery(st, placeholderValues, ScanConsistency.NOT_BOUNDED);
    JsonObject queryObject = query.n1ql();

    assertTrue(query instanceof ParameterizedN1qlQuery);
    assertEquals(st.toString(), query.statement().toString());
    assertEquals(N1qlParams.build().consistency(ScanConsistency.NOT_BOUNDED), query.params());
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

    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class), any(Class.class)))
        .thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Object.class);
    verify(mock).executeCollection(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldChooseEntityExecutionWhenEntityType() throws Exception {
    
    Method method = SampleRepository.class.getMethod("findById", Integer.class);
  	
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);
    
    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class), any(Class.class)))
        .thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Sample.class);
    verify(mock, never()).executeCollection(any(N1qlQuery.class), any(Class.class));
    verify(mock).executeEntity(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldChooseStreamExecutionWhenStreamType() throws Exception {
  	
    Method method = SampleRepository.class.getMethod("streamAll");
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);
    
    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        any(Class.class)))
        .thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Sample.class);
    verify(mock, never()).executeCollection(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class), any(Class.class));
    verify(mock).executeStream(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldChoosePagedExecutionWhenPageType() throws Exception {
  	
    Method method = SampleRepository.class.getMethod("findAllPaged", Pageable.class);
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);
    
    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        any(Class.class)))
        .thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Sample.class);

    verify(mock, never()).executeCollection(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class), any(Class.class));
    verify(mock).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldChooseSlicedExecutionWhenSliceType() throws Exception {

    Method method = SampleRepository.class.getMethod("findAllSliced", Pageable.class);
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);

    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        any(Class.class))).thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Sample.class);

    verify(mock, never()).executeCollection(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldExecuteSingleProjectionWhenRandomObjectReturnType() throws Exception {

    Method method = SampleRepository.class.getMethod("countDown");
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);

    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        any(Class.class))).thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Sample.class);

    verify(mock, never()).executeCollection(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldExecuteSingleProjectionWhenPrimitiveReturnType() throws Exception {
  	
    Method method = SampleRepository.class.getMethod("longMethod");
    CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, projectionFactory, context);

    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        any(Class.class))).thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, Sample.class);

    verify(mock, never()).executeCollection(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class), any(Class.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class), any(Class.class));
    verify(mock).executeSingleProjection(any(N1qlQuery.class));
  }

  static class Sample {
  		Integer id;
  }

  interface SampleRepository extends Repository<Sample, Integer> {

    Collection<Sample> findAll();

    Sample findById(Integer id);

    Stream<Sample> streamAll();

    Page<Sample> findAllPaged(Pageable pageable);

    Slice<Sample> findAllSliced(Pageable pageable);

    void modifyingMethod();

    CountDownLatch countDown();

    long longMethod();
  }
}
