package org.springframework.data.couchbase.repository.query;

import static com.couchbase.client.java.query.Select.select;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.ParametrizedQuery;
import com.couchbase.client.java.query.PreparedQuery;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryParams;
import com.couchbase.client.java.query.QueryPlan;
import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.SimpleQuery;
import com.couchbase.client.java.query.Statement;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.data.couchbase.UnitTestApplicationConfig;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.domain.Page;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.util.ReflectionUtils;

public class AbstractN1qlBasedQueryTest {

  @Test
  public void testQueryPlanShouldProducePreparedQuery() throws Exception {
    Statement st = select("*");
    QueryPlan plan = new QueryPlan(JsonObject.create());
    List<Object> params = new ArrayList<Object>(2);
    params.add("test");
    params.add(plan);

    Query query = AbstractN1qlBasedQuery.buildQuery(st, params.iterator());
    JsonObject queryObject = query.n1ql();
    assertTrue(query instanceof PreparedQuery);
    assertEquals(plan, query.statement());
    assertNull(query.params());
    assertNotNull(queryObject.get("args"));
    assertEquals(1, queryObject.getArray("args").size());
    assertEquals("test", queryObject.getArray("args").getString(0));

    params.add(QueryParams.build());
    query = AbstractN1qlBasedQuery.buildQuery(st, params.iterator());
    assertNotNull(query.params());
  }

  @Test
  public void testEmptyArgumentsShouldProduceSimpleQuery() throws Exception {
    Statement st = select("*");
    List<Object> params = Collections.emptyList();

    Query query = AbstractN1qlBasedQuery.buildQuery(st, params.iterator());
    JsonObject queryObject = query.n1ql();
    assertTrue(query instanceof SimpleQuery);
    assertEquals(st.toString(), query.statement().toString());
    assertNull(query.params());
    assertFalse(queryObject.containsKey("args"));
  }

  @Test
  public void testOnlyQueryParamsShouldProduceSimpleQuery() {
    Statement st = select("*");
    QueryParams queryParams = QueryParams.build().scanWait(1, TimeUnit.DAYS);
    List<Object> params = new ArrayList<Object>(1);
    params.add(queryParams);

    Query query = AbstractN1qlBasedQuery.buildQuery(st, params.iterator());
    JsonObject queryObject = query.n1ql();
    assertTrue(query instanceof SimpleQuery);
    assertEquals(st.toString(), query.statement().toString());
    assertEquals(queryParams, query.params());
    assertFalse(queryObject.containsKey("args"));
  }

  @Test
  public void testSimpleArgumentsShouldProduceParametrizedQuery() throws Exception {
    Statement st = select("*");
    List<Object> params = new ArrayList<Object>(2);
    params.add(123L);
    params.add("test");

    Query query = AbstractN1qlBasedQuery.buildQuery(st, params.iterator());
    JsonObject queryObject = query.n1ql();
    assertTrue(query instanceof ParametrizedQuery);
    assertEquals(st.toString(), query.statement().toString());
    assertNull(query.params());
    assertTrue(queryObject.containsKey("args"));
    JsonArray args = queryObject.getArray("args");
    assertEquals(2, args.size());
    assertEquals(123L, args.get(0));
    assertEquals("test", args.get(1));
  }

  @Test
  public void testSimpleArgumentsAndQueryParamsShouldProduceParametrizedQuery() throws Exception {
    Statement st = select("*");
    QueryParams queryParams = QueryParams.build().withContextId("toto");
    List<Object> params = new ArrayList<Object>(2);
    params.add(123L);
    params.add(queryParams);
    params.add("test");

    Query query = AbstractN1qlBasedQuery.buildQuery(st, params.iterator());
    JsonObject queryObject = query.n1ql();
    assertTrue(query instanceof ParametrizedQuery);
    assertEquals(st.toString(), query.statement().toString());
    assertEquals(queryParams, query.params());
    assertTrue(queryObject.containsKey("args"));
    JsonArray args = queryObject.getArray("args");
    assertEquals(2, args.size());
    assertEquals(123L, args.get(0));
    assertEquals("test", args.get(1));
  }

  @Test
  public void shouldChooseCollectionExecutionWhenCollectionType() {
    CouchbaseQueryMethod queryMethod = Mockito.mock(CouchbaseQueryMethod.class);
    Query query = Mockito.mock(Query.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(Query.class), any(QueryMethod.class), anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();
    when(queryMethod.isCollectionQuery()).thenReturn(true);

    mock.executeDependingOnType(query, queryMethod, false, false, false);
    verify(mock).executeCollection(any(Query.class));
    verify(mock, never()).executeEntity(any(Query.class));
    verify(mock, never()).executeStream(any(Query.class));
  }

  @Test
  public void shouldChooseEntityExecutionWhenEntityType() {
    CouchbaseQueryMethod queryMethod = Mockito.mock(CouchbaseQueryMethod.class);
    Query query = Mockito.mock(Query.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(Query.class), any(QueryMethod.class), anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();
    when(queryMethod.isQueryForEntity()).thenReturn(true);

    mock.executeDependingOnType(query, queryMethod, false, false, false);
    verify(mock, never()).executeCollection(any(Query.class));
    verify(mock).executeEntity(any(Query.class));
    verify(mock, never()).executeStream(any(Query.class));
  }

  @Test
  public void shouldChooseStreamExecutionWhenStreamType() {
    CouchbaseQueryMethod queryMethod = Mockito.mock(CouchbaseQueryMethod.class);
    Query query = Mockito.mock(Query.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(Query.class), any(QueryMethod.class), anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();
    when(queryMethod.isStreamQuery()).thenReturn(true);

    mock.executeDependingOnType(query, queryMethod, false, false, false);
    verify(mock, never()).executeCollection(any(Query.class));
    verify(mock, never()).executeEntity(any(Query.class));
    verify(mock).executeStream(any(Query.class));
  }

  @Test
  public void shouldThrowWhenUnsupportedType() throws NoSuchMethodException {
    CouchbaseQueryMethod queryMethod = Mockito.mock(CouchbaseQueryMethod.class);
    Query query = Mockito.mock(Query.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(Query.class), any(QueryMethod.class), anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();

    try { mock.executeDependingOnType(query, queryMethod, true, false, false); } catch (UnsupportedOperationException e) { }
    try { mock.executeDependingOnType(query, queryMethod, false, true, false); } catch (UnsupportedOperationException e) { }
    try { mock.executeDependingOnType(query, queryMethod, false, false, true); } catch (UnsupportedOperationException e) { }
    verify(mock, never()).executeCollection(any(Query.class));
    verify(mock, never()).executeEntity(any(Query.class));
    verify(mock, never()).executeStream(any(Query.class));
  }
}