package org.springframework.data.couchbase.repository;

import java.util.Date;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Point;


/**
 * An entity used to test conversion of parameters in query derivations.
 *
 * @author Simon Baslé
 */
public class Party {

  @Id
  private String key;

  private String name;

  //@Field("desc")
  @JsonProperty("desc")
  private String description;

  private Date eventDate;

  private long attendees;

  private Point location;

  public Party() {}

  public Party(String key, String name, String description, Date eventDate, long attendees, Point location) {
    this.key = key;
    this.name = name;
    this.description = description;
    this.eventDate = eventDate;
    this.attendees = attendees;
    this.location = location;
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
        ", location=" + location +
        '}';
  }
}
