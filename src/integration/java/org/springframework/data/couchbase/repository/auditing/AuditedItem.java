package org.springframework.data.couchbase.repository.auditing;

import java.util.Date;

import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;

@Document
public class AuditedItem {

  @Id
  private final String id;

  private String value;

  @CreatedBy
  private String creator;

  @LastModifiedBy
  private String lastModifiedBy;

  @LastModifiedDate
  private Date lastModification;

  @CreatedDate
  private Date creationDate;

  @Version
  private long version;

  public AuditedItem(String id, String value) {
    this.id = id;
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getCreator() {
    return creator;
  }

  public void setCreator(String creator) {
    this.creator = creator;
  }

  public String getLastModifiedBy() {
    return lastModifiedBy;
  }

  public void setLastModifiedBy(String lastModifiedBy) {
    this.lastModifiedBy = lastModifiedBy;
  }

  public Date getLastModification() {
    return lastModification;
  }

  public void setLastModification(Date lastModification) {
    this.lastModification = lastModification;
  }

  public Date getCreationDate() {
    return creationDate;
  }

  public void setCreationDate(Date creationDate) {
    this.creationDate = creationDate;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return "AuditedItem{" +
        "id='" + id + '\'' +
        ", value='" + value + '\'' +
        ", creator='" + creator + '\'' +
        ", lastModifiedBy='" + lastModifiedBy + '\'' +
        ", lastModification=" + lastModification +
        ", creationDate=" + creationDate +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AuditedItem that = (AuditedItem) o;

    if (!id.equals(that.id)) return false;
    if (!value.equals(that.value)) return false;
    if (creator != null ? !creator.equals(that.creator) : that.creator != null) return false;
    if (lastModifiedBy != null ? !lastModifiedBy.equals(that.lastModifiedBy) : that.lastModifiedBy != null)
      return false;
    if (lastModification != null ? !lastModification.equals(that.lastModification) : that.lastModification != null)
      return false;
    return creationDate != null ? creationDate.equals(that.creationDate) : that.creationDate == null;

  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + value.hashCode();
    result = 31 * result + (creator != null ? creator.hashCode() : 0);
    result = 31 * result + (lastModifiedBy != null ? lastModifiedBy.hashCode() : 0);
    result = 31 * result + (lastModification != null ? lastModification.hashCode() : 0);
    result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
    return result;
  }
}
