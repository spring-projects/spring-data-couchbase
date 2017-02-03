package org.springframework.data.couchbase.repository.auditing;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.Optional;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Simon Baslé
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = AuditedApplicationConfig.class)
public class AuditingTests {

  @Autowired
  private AuditedRepository repository;

  @Autowired
  private AuditedAuditorAware auditorAware;

  private static final String KEY = "auditedTest";

  @After
  public void cleanupAuditedEntity() {
    repository.getCouchbaseOperations().getCouchbaseBucket().remove(KEY);
  }

  @Test
  public void testCreationEventIsRegistered() {
    assertFalse(repository.exists(KEY));
    Date start = new Date();
    AuditedItem item = new AuditedItem(KEY, "creation");

    auditorAware.setAuditor("auditor");
    repository.save(item);
    Optional<AuditedItem> persisted = repository.findOne(KEY);

    assertTrue(persisted.isPresent());

    persisted.ifPresent(actual -> {

      assertNotNull("expected creation date audit trail", actual.getCreationDate());
      assertEquals("expected creation user audit trail", "auditor", actual.getCreator());

      assertTrue("creation date is too early", actual.getCreationDate().after(start));
      assertTrue("creation date is too late", actual.getCreationDate().before(new Date()));

      assertNull("expected modification date to be empty", actual.getLastModification());
      assertNull("expected modification user to be empty", actual.getLastModifiedBy());

      assertNotNull("expected version to be non null", actual.getVersion());
      assertTrue("expected version to be greater than 0", actual.getVersion() > 0L);
    });
  }

  @Test
  public void testUpdateEventIsRegistered() {
    assertFalse(repository.exists(KEY));

    String expectedCreator = "user1";
    String expectedUpdater = "user2";
    AuditedItem item = new AuditedItem(KEY, "creation");
    auditorAware.setAuditor(expectedCreator);

    repository.save(item);
    AuditedItem created = repository.findOne(KEY).orElse(null);

    auditorAware.setAuditor(expectedUpdater);
    repository.save(item);
    AuditedItem updated = repository.findOne(KEY).orElse(null);

    assertNotNull("expected entity to be persisted", updated);
    assertNotNull("expected creation date audit trail", updated.getCreationDate());
    assertEquals("expected creation user audit trail", expectedCreator, updated.getCreator());

    assertNotNull("expected modification date audit trail", updated.getLastModification());
    assertTrue("expected modification date to be after creation date", updated.getCreationDate().before(updated.getLastModification()));
    assertEquals("expected modification user to be the modifier", expectedUpdater, updated.getLastModifiedBy());

    assertNotNull("expected version to be non null", updated.getVersion());
    assertTrue("expected version to be greater than 0", updated.getVersion() > 0L);
    assertTrue("expected updated version to be different from the one at creation", created.getVersion() != updated.getVersion());
  }
}
