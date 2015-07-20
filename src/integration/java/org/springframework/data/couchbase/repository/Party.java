package org.springframework.data.couchbase.repository;

import java.util.Date;

import org.springframework.data.annotation.Id;
import com.couchbase.client.java.repository.annotation.Field;

/**
 * An entity used to test conversion of parameters in query derivations.
 *
 * @author Simon Baslé
 */
public class Party {

  @Id
  private final String key;

  private final String name;

  @Field("desc")
  private final String description;

  private final Date eventDate;

  private final long attendees;

  public Party(String key, String name, String description, Date eventDate, long attendees) {
    this.key = key;
    this.name = name;
    this.description = description;
    this.eventDate = eventDate;
    this.attendees = attendees;
  }

  public String getKey() {
    return key;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public Date getEventDate() {
    return eventDate;
  }

  public long getAttendees() {
    return attendees;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Party party = (Party) o;

    return key.equals(party.key);

  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public String toString() {
    return "Party{" +
        "name='" + name + '\'' +
        ", eventDate=" + eventDate +
        '}';
  }
}
