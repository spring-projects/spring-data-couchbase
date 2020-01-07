package org.springframework.data.couchbase.repository;

import static org.springframework.data.domain.Sort.Direction;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.query.QueryResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.support.N1qlCouchbaseRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.json.JsonObject;

public class RepositoryIndexUsageTest {

  private static final org.springframework.data.couchbase.core.query.Consistency CONSISTENCY = Consistency.STRONGLY_CONSISTENT;

  private CouchbaseOperations couchbaseOperations;
  private N1qlCouchbaseRepository<String, String> repository;

  @Before
  public void initMocks() {
    Bucket mockBucket = mock(Bucket.class);
    when(mockBucket.name()).thenReturn("mockBucket");

    CouchbaseConverter mockConverter = mock(CouchbaseConverter.class);
    when(mockConverter.getTypeKey()).thenReturn("mockType");
    when(mockConverter.convertForWriteIfNeeded(any(Object.class))).thenAnswer(
      invocation -> invocation.getArgument(0));

    couchbaseOperations = mock(CouchbaseOperations.class);
    when(couchbaseOperations.getDefaultConsistency()).thenReturn(CONSISTENCY);
    when(couchbaseOperations.getCouchbaseBucket()).thenReturn(mockBucket);
    when(couchbaseOperations.getConverter()).thenReturn(mockConverter);
    when(couchbaseOperations.findByN1QL(any(N1QLQuery.class), any(Class.class))).thenReturn(Collections.emptyList());

    // we test count, and it uses queryN1QL so lets mock that
    when(couchbaseOperations.queryN1QL(any(N1QLQuery.class))).thenReturn(null);

    CouchbaseEntityInformation metadata = mock(CouchbaseEntityInformation.class);
    when(metadata.getJavaType()).thenReturn(String.class);

    repository = new N1qlCouchbaseRepository<String, String>(metadata, couchbaseOperations);
  }

  @Test
  public void testFindAllKeysUsesQuery() {
/*    String expectedQueryParams = "ViewQuery(string/all){params=\"reduce=false&stale=false\", keys=\"[\"someKey\"]\"}";
    repository.findAllById(Collections.singleton("someKey"));

    verify(couchbaseOperations, atLeastOnce()).findByN1QL(any(N1QLQuery.class), any(Class.class));
    verify(couchbaseOperations, atLeastOnce()).queryN1QL(any(N1QLQuery.class));
     String sQuery = queryCaptor.getValue().toString();
    assertEquals(expectedQueryParams, sQuery);
  */}

  @Test
  public void testCountUsesViewWithConfiguredConsistencyAndReduces() {
/*    String expectedQueryParams = "ViewQuery(string/all){params=\"reduce=true&stale=false\"}";
    repository.count();

    verify(couchbaseOperations, never()).findByView(any(ViewQuery.class), any(Class.class));
    verify(couchbaseOperations, never()).findByN1QL(any(N1qlQuery.class), any(Class.class));
    verify(couchbaseOperations, never()).queryN1QL(any(N1qlQuery.class));
    ArgumentCaptor<ViewQuery> queryCaptor = ArgumentCaptor.forClass(ViewQuery.class);
    verify(couchbaseOperations).queryView(queryCaptor.capture());
    String sQuery = queryCaptor.getValue().toString();
    assertEquals(expectedQueryParams, sQuery);
  */
  }

  @Test
  public void testCountParsesAndAddsLongValuesFromRows() {
    QueryResult mockCount = mock(QueryResult.class);
    when(mockCount.rowsAsObject()).thenReturn(Arrays.asList(JsonObject.create().put("$1", 10)));
    when(couchbaseOperations.queryN1QL(any(N1QLQuery.class))).thenReturn(mockCount);
    long count = repository.count();
    assertEquals(10L, count);
  }

  @Test
  public void testDeleteAllUsesViewWithConfiguredConsistency() {
  /*  String expectedQueryParams = "ViewQuery(string/all){params=\"reduce=false&stale=false\"}";
    repository.deleteAll();

    verify(couchbaseOperations, never()).findByView(any(ViewQuery.class), any(Class.class));
    verify(couchbaseOperations, never()).findByN1QL(any(N1qlQuery.class), any(Class.class));
    verify(couchbaseOperations, never()).queryN1QL(any(N1qlQuery.class));
    ArgumentCaptor<ViewQuery> queryCaptor = ArgumentCaptor.forClass(ViewQuery.class);
    verify(couchbaseOperations).queryView(queryCaptor.capture());
    String sQuery = queryCaptor.getValue().toString();
    assertEquals(expectedQueryParams, sQuery);
  */
  }

  @Test
  public void testFindAllSortedUsesN1qlWithConfiguredConsistencyAndOrderBy() {
    String expectedOrderClause = "ORDER BY `length` ASC";
    Sort sort = Sort.by(Direction.ASC, "length");
    repository.findAll(sort);

    verify(couchbaseOperations, never()).queryN1QL(any(N1QLQuery.class));
    ArgumentCaptor<N1QLQuery> queryCaptor = ArgumentCaptor.forClass(N1QLQuery.class);
    verify(couchbaseOperations).findByN1QL(queryCaptor.capture(), any(Class.class));

    JsonObject query = queryCaptor.getValue().n1ql();
    assertEquals(CONSISTENCY.n1qlConsistency().toString(), query.getString("scan_consistency"));
    String statement = query.getString("statement");
    assertTrue("Expected " + expectedOrderClause + " in " + statement, statement.contains(expectedOrderClause));
  }

  @Test
  public void testFindAllPagedUsesUsesN1qlConfiguredConsistencyAndLimitOffset() {
    String expectedLimitClause = "LIMIT 10 OFFSET 0";
    repository.findAll(PageRequest.of(0, 10));

    verify(couchbaseOperations, never()).queryN1QL(any(N1QLQuery.class));
    ArgumentCaptor<N1QLQuery> queryCaptor = ArgumentCaptor.forClass(N1QLQuery.class);
    verify(couchbaseOperations).findByN1QL(queryCaptor.capture(), any(Class.class));

    JsonObject query = queryCaptor.getValue().n1ql();
    assertEquals(CONSISTENCY.n1qlConsistency().toString(), query.getString("scan_consistency"));
    String statement = query.getString("statement");
    assertTrue("Expected " + expectedLimitClause + " in " + statement, statement.contains(expectedLimitClause));
  }

  /*
  @Test
  public void testDeleteAllSwallowsDocumentDoesNotExistException() {
     doThrow(new DataRetrievalFailureException("ignored", new DocumentNotFoundException("id1"))).when(couchbaseOperations).remove("id1");
    doThrow(new DataRetrievalFailureException("ignored", DocumentNotFoundException.forKey("id2"))).when(couchbaseOperations).remove("id1");
    try {
      repository.deleteAll();
    } catch (DataRetrievalFailureException e) {
      // this should never actually be thrown, since we are never calling remove (which we used to do)
      fail("Expected DataRetrievalFailureException on id2");
    }
    verify(couchbaseOperations, never()).remove("id1");
    verify(couchbaseOperations, never()).remove("id2");
  }
  */

}
