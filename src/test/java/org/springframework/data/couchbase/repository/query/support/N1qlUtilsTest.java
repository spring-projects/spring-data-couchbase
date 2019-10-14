package org.springframework.data.couchbase.repository.query.support;

import static com.couchbase.client.java.query.dsl.Expression.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.couchbase.core.Beer;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.CountFragment;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.core.EntityMetadata;

public class N1qlUtilsTest {

  @Test
  public void testEscapedBucket() throws Exception {
    String bucketName = "b";
    String expected = "`b`";

    String real = N1qlUtils.escapedBucket(bucketName).toString();

    assertThat(real).isEqualTo(expected);
  }

  @Test
  public void testCreateSelectClauseForEntity() throws Exception {
    String expected = "SELECT META(`b`).id AS _ID, META(`b`).cas AS _CAS, `b`.*";
    String real = N1qlUtils.createSelectClauseForEntity("b").toString();

    assertThat(real).isEqualTo(expected);
  }

  @Test
  public void testCreateSelectFromForEntity() throws Exception {
    String expected = "SELECT META(`b`).id AS _ID, META(`b`).cas AS _CAS, `b`.* FROM `b`";
    String real = N1qlUtils.createSelectFromForEntity("b").toString();

    assertThat(real).isEqualTo(expected);
  }

  @Test
  public void testCreateWhereFilterForEntity() throws Exception {
    String expected = "`_class` = \"java.lang.String\"";
    CouchbaseConverter converter = mock(CouchbaseConverter.class);
    when(converter.getTypeKey()).thenReturn("_class");
    EntityMetadata metadata = mock(EntityMetadata.class);
    when(metadata.getJavaType()).thenReturn(String.class);

    String real = N1qlUtils.createWhereFilterForEntity(null, converter, metadata).toString();

    assertThat(real).isEqualTo(expected);
  }

  @Test
  public void testCreateWhereFilterForEntityTakesTypeKeyIntoAccount() throws Exception {
    String expected = "`hohoho` = \"java.lang.String\"";
    CouchbaseConverter converter = mock(CouchbaseConverter.class);
    when(converter.getTypeKey()).thenReturn("hohoho");
    EntityMetadata metadata = mock(EntityMetadata.class);
    when(metadata.getJavaType()).thenReturn(String.class);

    String real = N1qlUtils.createWhereFilterForEntity(null, converter, metadata).toString();

    assertThat(real).isEqualTo(expected);
  }

  @Test
  public void testGetPathWithAlternativeFieldNamesCallsMapperOnce() throws Exception {
    CouchbaseConverter converter = mock(CouchbaseConverter.class);
    PropertyPath descPropertyPath = PropertyPath.from("active", Beer.class);
    MappingContext mockMapping = mock(CouchbaseMappingContext.class);
    when(converter.getMappingContext()).thenReturn(mockMapping);

    N1qlUtils.getPathWithAlternativeFieldNames(converter, descPropertyPath);
    verify(mockMapping).getPersistentPropertyPath(descPropertyPath);
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
        N1qlUtils.createSort(Sort.by("description", "attendees"), converter);

    assertThat(realSort.length).isEqualTo(2);
    assertThat(realSort[0].toString())
			.isEqualTo(com.couchbase.client.java.query.dsl.Sort.asc("`description`")
					.toString());
    assertThat(realSort[1].toString())
			.isEqualTo(com.couchbase.client.java.query.dsl.Sort.asc("`attendees`")
					.toString());

    verifyZeroInteractions(converter);
  }

  @Test
  public void testCreateSortIgnoresCaseWhenSpecified() throws Exception {
    CouchbaseConverter converter = mock(CouchbaseConverter.class);
    Sort sortDescription = Sort.by(Order.asc("description").ignoreCase(), Order.asc("attendees"));
    com.couchbase.client.java.query.dsl.Sort[] realSort = N1qlUtils.createSort(sortDescription, converter);

    assertThat(realSort.length).isEqualTo(2);
    assertThat(realSort[0].toString()).isEqualTo(com.couchbase.client.java.query.dsl.Sort
			.asc("LOWER(TOSTRING(`description`))").toString());
    assertThat(realSort[1].toString())
			.isEqualTo(com.couchbase.client.java.query.dsl.Sort.asc("`attendees`")
					.toString());

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

    assertThat(real).isEqualTo(expectedDefault);
    assertThat(realWithTypeKey).isEqualTo(expectedTypeKey);
  }

  @Test
  public void testCreateWhereFilterForEntityWithBaseCriteria() throws Exception {
    String expected = "(field1 >= 30 OR field2 = \"foo\") AND `_class` = \"java.lang.String\"";
    CouchbaseConverter converter = mock(CouchbaseConverter.class);
    when(converter.getTypeKey()).thenReturn("_class");
    EntityMetadata metadata = mock(EntityMetadata.class);
    when(metadata.getJavaType()).thenReturn(String.class);

    String real = N1qlUtils.createWhereFilterForEntity(
            x("field1").gte(30).or(x("field2").eq(s("foo"))),
            converter, metadata).toString();

    assertThat(real).isEqualTo(expected);
  }

  @Test
  public void testOrderByWithNestedField() throws Exception {
    CouchbaseConverter converter = mock(CouchbaseConverter.class);
    com.couchbase.client.java.query.dsl.Sort[] realSort =
            N1qlUtils.createSort(Sort.by("party.attendees"), converter);

    assertThat(realSort[0].toString()).isEqualTo("`party`.`attendees` ASC");
    verifyZeroInteractions(converter);
  }
}
