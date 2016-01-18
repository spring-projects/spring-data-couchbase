package org.springframework.data.couchbase.repository.query;

import static com.couchbase.client.java.query.Select.select;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

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

import org.springframework.data.couchbase.core.mapping.event.User;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.QueryCreationException;
import org.springframework.data.repository.query.QueryMethod;

public class AbstractN1qlBasedQueryTest {

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
  public void shouldChooseCollectionExecutionWhenCollectionType() {
    CouchbaseQueryMethod queryMethod = Mockito.mock(CouchbaseQueryMethod.class);
    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();
    when(queryMethod.isCollectionQuery()).thenReturn(true);

    mock.executeDependingOnType(query, query, queryMethod, pageable, false, false, false);
    verify(mock).executeCollection(any(N1qlQuery.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldChooseEntityExecutionWhenEntityType() {
    CouchbaseQueryMethod queryMethod = Mockito.mock(CouchbaseQueryMethod.class);
    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();
    when(queryMethod.isQueryForEntity()).thenReturn(true);

    mock.executeDependingOnType(query, query, queryMethod, pageable, false, false, false);
    verify(mock, never()).executeCollection(any(N1qlQuery.class));
    verify(mock).executeEntity(any(N1qlQuery.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldChooseStreamExecutionWhenStreamType() {
    CouchbaseQueryMethod queryMethod = Mockito.mock(CouchbaseQueryMethod.class);
    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();
    when(queryMethod.isStreamQuery()).thenReturn(true);

    mock.executeDependingOnType(query, query, queryMethod, pageable, false, false, false);
    verify(mock, never()).executeCollection(any(N1qlQuery.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class));
    verify(mock).executeStream(any(N1qlQuery.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldChoosePagedExecutionWhenPageType() {
    CouchbaseQueryMethod queryMethod = Mockito.mock(CouchbaseQueryMethod.class);
    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, true, false, false);
    verify(mock, never()).executeCollection(any(N1qlQuery.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class));
    verify(mock).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldChooseSlicedExecutionWhenSliceType() {
    CouchbaseQueryMethod queryMethod = Mockito.mock(CouchbaseQueryMethod.class);
    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();
    when(queryMethod.isSliceQuery()).thenReturn(true);

    mock.executeDependingOnType(query, query, queryMethod, pageable, false, true, false);
    verify(mock, never()).executeCollection(any(N1qlQuery.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldThrowWhenModifyingType() {
    CouchbaseQueryMethod queryMethod = Mockito.mock(CouchbaseQueryMethod.class);
    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();

    try { mock.executeDependingOnType(query, query, queryMethod, pageable, false, false, true); fail(); } catch (UnsupportedOperationException e) { }
    verify(mock, never()).executeCollection(any(N1qlQuery.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldExecuteSingleProjectionWhenRandomObjectReturnType() {
    CouchbaseQueryMethod queryMethod = Mockito.mock(CouchbaseQueryMethod.class);
    doReturn(CountDownLatch.class).when(queryMethod).getReturnedObjectType();

    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, false, false, false);
    verify(mock, never()).executeCollection(any(N1qlQuery.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock).executeSingleProjection(any(N1qlQuery.class));
  }

  @Test
  public void shouldExecuteSingleProjectionWhenPrimitiveReturnType() {
    CouchbaseQueryMethod queryMethod = Mockito.mock(CouchbaseQueryMethod.class);
    doReturn(long.class).when(queryMethod).getReturnedObjectType();

    N1qlQuery query = Mockito.mock(N1qlQuery.class);
    Pageable pageable = Mockito.mock(Pageable.class);
    AbstractN1qlBasedQuery mock = mock(AbstractN1qlBasedQuery.class);
    when(mock.executeDependingOnType(any(N1qlQuery.class), any(N1qlQuery.class), any(QueryMethod.class), any(Pageable.class),
        anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();

    mock.executeDependingOnType(query, query, queryMethod, pageable, false, false, false);
    verify(mock, never()).executeCollection(any(N1qlQuery.class));
    verify(mock, never()).executeEntity(any(N1qlQuery.class));
    verify(mock, never()).executeStream(any(N1qlQuery.class));
    verify(mock, never()).executePaged(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock, never()).executeSliced(any(N1qlQuery.class), any(N1qlQuery.class), any(Pageable.class));
    verify(mock).executeSingleProjection(any(N1qlQuery.class));
  }
}
