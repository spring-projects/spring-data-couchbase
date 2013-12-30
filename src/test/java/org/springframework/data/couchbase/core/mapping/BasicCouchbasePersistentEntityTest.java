package org.springframework.data.couchbase.core.mapping;

import org.junit.Test;
import org.springframework.data.util.ClassTypeInformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BasicCouchbasePersistentEntityTest {

  @Test
  public void doesNotUseGetExpiry() throws Exception {
    assertEquals(0, getBasicCouchbasePersistentEntity(SimpleDocument.class).getExpiry());
  }

  @Test
  public void usesGetExpiry() throws Exception {
    assertEquals(10, getBasicCouchbasePersistentEntity(SimpleDocumentWithExpiry.class).getExpiry());
  }

  @Test
  public void doesNotUseIsUpdateExpiryForRead() throws Exception {
    assertFalse(getBasicCouchbasePersistentEntity(SimpleDocument.class).isUpdateExpiryForRead());
    assertFalse(getBasicCouchbasePersistentEntity(SimpleDocumentWithExpiry.class).isUpdateExpiryForRead());
  }

  @Test
  public void usesIsUpdateExpiryForRead() throws Exception {
    assertTrue(getBasicCouchbasePersistentEntity(SimpleDocumentWithExpiryUpdatedOnRead.class).isUpdateExpiryForRead());
  }

  private BasicCouchbasePersistentEntity getBasicCouchbasePersistentEntity(Class<?> clazz) {
    return new BasicCouchbasePersistentEntity(ClassTypeInformation.from(clazz));
  }

  public static class SimpleDocument {
  }

  @Document(expiry = 10)
  public static class SimpleDocumentWithExpiry {
  }

  @Document(expiry = 10, updateExpiryForRead = true)
  public static class SimpleDocumentWithExpiryUpdatedOnRead {
  }
}
