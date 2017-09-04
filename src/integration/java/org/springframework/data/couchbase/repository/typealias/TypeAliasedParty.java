/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.typealias;

import com.couchbase.client.java.repository.annotation.Field;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.geo.Point;

import java.util.Date;

/**
 * An entity used to test type alias works with all type of queries
 *
 * @author Maxence Labusquiere
 */
@TypeAlias("MyTypeAlias")
public class TypeAliasedParty {

  @Id
  private final String key;

  private final String name;

  @Field("desc")
  private final String description;

  private final Date eventDate;

  private final long attendees;

  private final Point location;

  public TypeAliasedParty(String key, String name, String description, Date eventDate, long attendees, Point location) {
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
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    TypeAliasedParty party = (TypeAliasedParty) o;

    return key.equals(party.key);
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public String toString() {
    return "TypeAliasedParty{" + "name='" + name + '\'' + ", eventDate=" + eventDate + ", location=" + location + '}';
  }
}
