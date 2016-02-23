package org.springframework.data.couchbase.repository.auditing;

import org.springframework.data.domain.AuditorAware;
import org.springframework.stereotype.Component;

public class AuditedAuditorAware implements AuditorAware<String> {

  private String auditor = "auditor";

  @Override
  public String getCurrentAuditor() {
    return auditor;
  }

  public void setAuditor(String auditor) {
    this.auditor = auditor;
  }
}
