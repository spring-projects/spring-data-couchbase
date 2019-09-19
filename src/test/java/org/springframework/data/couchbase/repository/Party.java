package org.springframework.data.couchbase.repository;

import java.util.Date;

import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.annotation.Field;
import org.springframework.data.geo.Point;


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

  private final Point location;

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

  public Point getLocation() {
    return location;
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
