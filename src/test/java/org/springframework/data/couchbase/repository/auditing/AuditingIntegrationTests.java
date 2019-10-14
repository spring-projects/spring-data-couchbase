package org.springframework.data.couchbase.repository.auditing;

import java.util.Date;
import java.util.Optional;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.TestContainerResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 * @author Mark Paluch
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = AuditedApplicationConfig.class)
public class AuditingIntegrationTests {

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
    assertThat(repository.existsById(KEY)).isFalse();
    Date start = new Date();
    AuditedItem item = new AuditedItem(KEY, "creation");

    auditorAware.setAuditor("auditor");
    repository.save(item);
    Optional<AuditedItem> persisted = repository.findById(KEY);

    assertThat(persisted.isPresent()).isTrue();

    persisted.ifPresent(actual -> {

      assertThat(actual.getCreationDate()).as("expected creation date audit trail")
			  .isNotNull();
      assertThat(actual.getCreator()).as("expected creation user audit trail")
			  .isEqualTo("auditor");

      assertThat(actual.getCreationDate().after(start)).as("creation date is too early")
			  .isTrue();
      assertThat(actual.getCreationDate().before(new Date()))
			  .as("creation date is too late").isTrue();

      assertThat(actual.getLastModification())
			  .as("expected modification date to be empty").isNull();
      assertThat(actual.getLastModifiedBy()).as("expected modification user to be empty")
			  .isNull();

      assertThat(actual.getVersion()).as("expected version to be non null").isNotNull();
      assertThat(actual.getVersion() > 0L).as("expected version to be greater than 0")
			  .isTrue();
    });
  }

  @Test
  public void testUpdateEventIsRegistered() {
    assertThat(repository.existsById(KEY)).isFalse();

    String expectedCreator = "user1";
    String expectedUpdater = "user2";
    AuditedItem item = new AuditedItem(KEY, "creation");
    auditorAware.setAuditor(expectedCreator);

    repository.save(item);
    AuditedItem created = repository.findById(KEY).orElse(null);

    auditorAware.setAuditor(expectedUpdater);
    repository.save(item);
    AuditedItem updated = repository.findById(KEY).orElse(null);

    assertThat(updated).as("expected entity to be persisted").isNotNull();
    assertThat(updated.getCreationDate()).as("expected creation date audit trail")
			.isNotNull();
    assertThat(updated.getCreator()).as("expected creation user audit trail")
			.isEqualTo(expectedCreator);

    assertThat(updated.getLastModification()).as("expected modification date audit trail")
			.isNotNull();
    assertThat(updated.getCreationDate().before(updated.getLastModification()))
			.as("expected modification date to be after creation date").isTrue();
    assertThat(updated.getLastModifiedBy())
			.as("expected modification user to be the modifier")
			.isEqualTo(expectedUpdater);

    assertThat(updated.getVersion()).as("expected version to be non null").isNotNull();
    assertThat(updated.getVersion() > 0L).as("expected version to be greater than 0")
			.isTrue();
    assertThat(created.getVersion() != updated.getVersion())
			.as("expected updated version to be different from the one at creation")
			.isTrue();
  }
}
