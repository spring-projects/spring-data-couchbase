package org.springframework.data.couchbase.repository.query.support;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.repository.Party;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.CountFragment;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.repository.core.EntityMetadata;

public class N1qlUtilsTest {

  @Test
  public void testEscapedBucket() throws Exception {
    String bucketName = "b";
    String expected = "`b`";

    String real = N1qlUtils.escapedBucket(bucketName).toString();

    assertEquals(expected, real);
  }

  @Test
  public void testCreateSelectClauseForEntity() throws Exception {
    String expected = "SELECT META(`b`).id AS _ID, META(`b`).cas AS _CAS, `b`.*";
    String real = N1qlUtils.createSelectClauseForEntity("b").toString();

    assertEquals(expected, real);
  }

  @Test
  public void testCreateSelectFromForEntity() throws Exception {
    String expected = "SELECT META(`b`).id AS _ID, META(`b`).cas AS _CAS, `b`.* FROM `b`";
    String real = N1qlUtils.createSelectFromForEntity("b").toString();

    assertEquals(expected, real);
  }

  @Test
  public void testCreateWhereFilterForEntity() throws Exception {
    String expected = "`_class` = \"java.lang.String\"";
    CouchbaseConverter converter = mock(CouchbaseConverter.class);
    when(converter.getTypeKey()).thenReturn("_class");
    EntityMetadata metadata = mock(EntityMetadata.class);
    when(metadata.getJavaType()).thenReturn(String.class);

    String real = N1qlUtils.createWhereFilterForEntity(null, converter, metadata).toString();

    assertEquals(expected, real);
  }

  @Test
  public void testCreateWhereFilterForEntityTakesTypeKeyIntoAccount() throws Exception {
    String expected = "`hohoho` = \"java.lang.String\"";
    CouchbaseConverter converter = mock(CouchbaseConverter.class);
    when(converter.getTypeKey()).thenReturn("hohoho");
    EntityMetadata metadata = mock(EntityMetadata.class);
    when(metadata.getJavaType()).thenReturn(String.class);

    String real = N1qlUtils.createWhereFilterForEntity(null, converter, metadata).toString();

    assertEquals(expected, real);
  }

  @Test
  public void testGetPathWithAlternativeFieldNames() throws Exception {
    CouchbaseConverter converter = mock(CouchbaseConverter.class);
    PropertyPath partyDescPropertyPath = PropertyPath.from("description", Party.class);
    MappingContext mockMapping = mock(CouchbaseMappingContext.class);
    when(converter.getMappingContext()).thenReturn(mockMapping);

    N1qlUtils.getPathWithAlternativeFieldNames(converter, partyDescPropertyPath);
    verify(mockMapping).getPersistentPropertyPath(partyDescPropertyPath);
    verifyNoMoreInteractions(mockMapping);
  }

  @Test
  @Ignore("rather test the dotted path converter")
  public void testGetDottedPathWithAlternativeFieldNames() throws Exception {

  }

  @Test
  public void testCreateSortUsesPropertiesAsIsWithEscaping() throws Exception {
    CouchbaseConverter converter = mock(CouchbaseConverter.class);
    com.couchbase.client.java.query.dsl.Sort[] realSort =
        N1qlUtils.createSort(new Sort("description", "attendees"), converter);

    assertEquals(2, realSort.length);
    assertEquals(com.couchbase.client.java.query.dsl.Sort.asc("`description`").toString(), realSort[0].toString());
    assertEquals(com.couchbase.client.java.query.dsl.Sort.asc("`attendees`").toString(), realSort[1].toString());

    verifyZeroInteractions(converter);
  }

  @Test
  public void testCreateCountQueryForEntity() throws Exception {
    CouchbaseConverter converter = mock(CouchbaseConverter.class);
    when(converter.getTypeKey()).thenReturn("_class", "otherField");
    CouchbaseEntityInformation entityInformation = mock(CouchbaseEntityInformation.class);
    when(entityInformation.getJavaType()).thenReturn(String.class);

    String expectedDefault = "SELECT COUNT(*) AS " + CountFragment.COUNT_ALIAS + " FROM `b` WHERE `_class` = \"java.lang.String\"";
    String expectedTypeKey = "SELECT COUNT(*) AS " + CountFragment.COUNT_ALIAS + " FROM `b` WHERE `otherField` = \"java.lang.String\"";
    String real = N1qlUtils.createCountQueryForEntity("b", converter, entityInformation).toString();
    String realWithTypeKey = N1qlUtils.createCountQueryForEntity("b", converter, entityInformation).toString();

    assertEquals(expectedDefault, real);
    assertEquals(expectedTypeKey, realWithTypeKey);
  }
}