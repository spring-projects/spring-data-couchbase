package org.springframework.data.couchbase.repository.auditing;

import org.springframework.data.domain.AuditorAware;

import java.util.Optional;

public class AuditedAuditorAware implements AuditorAware<String> {

  private Optional<String> auditor = Optional.of("auditor");

  @Override
  public Optional<String> getCurrentAuditor() {
    return auditor;
  }

  public void setAuditor(String auditor) {
    this.auditor = Optional.of(auditor);
  }
}
