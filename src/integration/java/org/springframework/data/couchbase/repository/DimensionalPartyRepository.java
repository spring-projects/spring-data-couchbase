package org.springframework.data.couchbase.repository;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.List;

import com.couchbase.client.java.document.json.JsonArray;

import org.springframework.data.couchbase.core.query.Dimensional;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.repository.CrudRepository;

public interface DimensionalPartyRepository extends CrudRepository<Party, String> {

  @Dimensional(designDocument = "partyGeo", spatialViewName = "byLocation")
  List<Party> findByLocationNear(Point p, Distance d);

  @Dimensional(designDocument = "partyGeo", spatialViewName = "byLocation")
  List<Party> findByLocationWithin(Box boundingBox);

  @Dimensional(designDocument = "partyGeo", spatialViewName = "byLocation")
  List<Party> findByLocationWithin(Polygon zone);

  @Dimensional(designDocument = "partyGeo", spatialViewName = "byLocation")
  List<Party> findByLocationWithin(Point[] points);

  @Dimensional(designDocument = "partyGeo", spatialViewName = "byLocation")
  List<Party> findByLocationWithin(Point point);

  @Dimensional(designDocument = "partyGeo", spatialViewName = "byLocation")
  List<Party> findByLocationWithin(JsonArray range);

  @Dimensional(designDocument = "partyGeo", spatialViewName = "byLocation")
  List<Party> findByLocationWithin(Point lowerLeft, Point upperRight);

  @Dimensional(designDocument = "partyGeo", spatialViewName = "byLocationAndAttendees", dimensions = 3)
  List<Party> findByLocationWithin(Circle zone);

  @Dimensional(designDocument = "partyGeo", spatialViewName = "byLocationAndAttendees", dimensions = 3)
  List<Party> findByLocationWithinAndAttendeesGreaterThan(Polygon zone, double minAttendees);

  @Dimensional(designDocument = "partyGeo", spatialViewName = "byLocationAndAttendees", dimensions = 3)
  List<Party> findByLocationWithin(JsonArray startRange, JsonArray endRange);

  //TODO more coverage of operators?

  @IndexedByLocation
  List<Party> findByLocationIsWithin(Point a, Point b);

  @Dimensional(designDocument = "partyGeo", spatialViewName = "byLocation", dimensions = 2)
  @Retention(RetentionPolicy.RUNTIME)
  @interface IndexedByLocation { }
}
