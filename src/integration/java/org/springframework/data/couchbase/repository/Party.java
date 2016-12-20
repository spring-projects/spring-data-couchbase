package org.springframework.data.couchbase.repository;

import java.util.Date;

import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Point;

import com.couchbase.client.java.repository.annotation.Field;
import org.springframework.data.repository.query.parser.Part;

/**
 * An entity used to test conversion of parameters in query derivations.
 *
 * @author Simon Baslé
 */
public class Party {

  @Id
  private String key;

  private String name;

  @Field("desc")
  private String description;

  private Date eventDate;

  private long attendees;

  private Point location;

  public Party() {
    this(null, null, null, null, 0, null);
  }

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

  public void setKey(String key) {
    this.key = key;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }


  public void setDescription(String description) {
    this.description = description;
  }

  public Date getEventDate() {
    return eventDate;
  }

  public void setEventDate(Date date) {
    this.eventDate = date;
  }

  public long getAttendees() {
    return attendees;
  }

  public void setAttendees(long attendees) {
    this.attendees = attendees;
  }

  public Point getLocation() {
    return location;
  }

  public void setLocation(Point location) {
    this.location = location;
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
