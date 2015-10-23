package org.springframework.data.couchbase.repository;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.couchbase.client.java.document.json.JsonArray;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.CouchbaseQueryExecutionException;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Simon Baslé
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(PartyPopulatorListener.class)
public class DimensionalQueryTests {

  @Autowired
  private RepositoryOperationsMapping templateMapping;

  @Autowired
  private IndexManager indexManager;

  private DimensionalPartyRepository repository;

  @Before
  public void setup() throws Exception {
    RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(templateMapping, indexManager);
    repository = factory.getRepository(DimensionalPartyRepository.class);
  }

  @Test
  public void testFindWithingPolygon() {
    Set<String> expectedKeys = new HashSet<String>();
    expectedKeys.add("testparty-2");
    expectedKeys.add("testparty-3");
    expectedKeys.add("testparty-4");
    expectedKeys.add("testparty-5");
    //a zone that engulfs parties 2, 3, 4 and 5, in the shape of a downward arrow pointing right
    Polygon zone = new Polygon(new Point(1, -2),
        new Point(3, -1.5),
        new Point(6, -4),
        new Point(5.5, -5.5),
        new Point(3, -5));

    List<Party> parties = repository.findByLocationWithin(zone);

    assertEquals(4, parties.size());
    for (Party party : parties) {
      assertTrue(expectedKeys.contains(party.getKey()));
    }
  }

  @Test
  public void testByLocationNear() {
    Set<String> expectedKeys = new HashSet<String>();
    expectedKeys.add("testparty-0");
    expectedKeys.add("testparty-1");

    List<Party> parties = repository.findByLocationNear(new Point(0, 0), new Distance(1.5));
    assertEquals(2, parties.size());
    for (Party party : parties) {
      assertTrue(expectedKeys.contains(party.getKey()));
    }

    parties = repository.findByLocationNear(new Point(0, 0), new Distance(2.5));
    expectedKeys.add("testparty-2");
    assertEquals(3, parties.size());
    for (Party party : parties) {
      assertTrue(expectedKeys.contains(party.getKey()));
    }
  }

  @Test
  public void testByLocationAndSparseAttendees() {
    Set<String> expectedKeys = new HashSet<String>();
    expectedKeys.add("testparty-4");
    expectedKeys.add("testparty-5");
    //a zone that engulfs parties 2, 3, 4 and 5 (with resp. 120, 130, 140 and 150 attendees)
    //in the shape of a downward arrow pointing right
    Polygon zone = new Polygon(new Point(1, -2),
        new Point(3, -1.5),
        new Point(6, -4),
        new Point(5.5, -5.5),
        new Point(3, -5));

    //first check the zone contains 4 parties
    List<Party> allPartiesInZone = repository.findByLocationWithinAndAttendeesGreaterThan(zone, -1);
    List<Party> allPartiesInZoneWithoutAttendeeCriteria = repository.findByLocationWithin(zone);
    assertEquals(allPartiesInZone.toString(), 4, allPartiesInZone.size());
    assertEquals(allPartiesInZoneWithoutAttendeeCriteria, allPartiesInZone);

    //check parties are limited by the attendees
    List<Party> parties = repository.findByLocationWithinAndAttendeesGreaterThan(zone, 140);
    for (Party party : parties) {
      System.out.println(party.getKey() + " : " + party.getLocation() + " " + party.getAttendees());
    }

    assertEquals(parties.toString(), 2, parties.size());
    for (Party party : parties) {
      assertTrue(party.getAttendees() >= 140);
      assertTrue(expectedKeys.contains(party.getKey()));
    }
  }

  @Test
  public void testByLocationInCircle() {
    Circle zone1 = new Circle(new Point(-5,5), new Distance(5.5));
    Circle zone2 = new Circle(new Point(6, -6), new Distance(10));
    Circle zoneEmpty = new Circle(new Point(6,6), new Distance(3));

    List<Party> parties = repository.findByLocationWithin(zone1);

    assertEquals(1, parties.size());
    assertEquals("testparty-0", parties.get(0).getKey());

    parties = repository.findByLocationWithin(zone2);

    assertEquals(12, parties.size()); //all the parties except the special one at 100, 100

    parties = repository.findByLocationWithin(zoneEmpty);
    assertEquals(0, parties.size());
  }

  @Test
  public void testByLocationInBox() {
    Box zone1 = new Box(new Point(-10.5, -0.5), new Point(0.5, 10.5));
    Box zone2 = new Box(new Point(-4, -16), new Point(16, 4));
    Box zoneEmpty = new Box(new Point(3, 3), new Point(9, 9));

    List<Party> parties = repository.findByLocationWithin(zone1);

    assertEquals(1, parties.size());
    assertEquals("testparty-0", parties.get(0).getKey());

    parties = repository.findByLocationWithin(zone2);

    assertEquals(12, parties.size()); //all the parties except the special one at 100, 100

    parties = repository.findByLocationWithin(zoneEmpty);
    assertEquals(0, parties.size());
  }

  @Test
  public void testByLocationInPolygon() {
    //bounding box of (-10.5, -0.5),(0.5, 10.5)
    Polygon zone1 = new Polygon(
        new Point(-10.5, -0.5),
        new Point(-10.5, 10.5),
        new Point(0.5, 10.5),
        new Point(0.4, 1));
    //bounding box of (-4, -16),(16,4)
    Polygon zone2 = new Polygon(
        new Point(-4, -10),
        new Point(-3, 4),
        new Point(14, 2),
        new Point(16, -16));
    //bounding box of (3,3)(9,9)
    Polygon zoneEmpty = new Polygon(
        new Point(3, 3),
        new Point(3, 7),
        new Point(6, 7),
        new Point(6, 9),
        new Point(9, 9),
        new Point(9, 5),
        new Point(6, 5),
        new Point(6, 3));

    List<Party> parties = repository.findByLocationWithin(zone1);

    assertEquals(1, parties.size());
    assertEquals("testparty-0", parties.get(0).getKey());

    parties = repository.findByLocationWithin(zone2);

    assertEquals(12, parties.size()); //all the parties except the special one at 100, 100

    parties = repository.findByLocationWithin(zoneEmpty);
    assertEquals(0, parties.size());
  }

  @Test
  public void testByLocationWithinTwoPoints() {
    Point zone1LowerLeft = new Point(-10.5, -0.5);
    Point zone1UpperRight = new Point(0.5, 10.5);
    Point zone2LowerLeft = new Point(-4, -16);
    Point zone2UpperRight = new Point(16, 4);
    Point zoneEmptyLowerLeft = new Point(3, 3);
    Point zoneEmptyUpperRight = new Point(9, 9);

    List<Party> parties = repository.findByLocationWithin(zone1LowerLeft, zone1UpperRight);

    assertEquals(1, parties.size());
    assertEquals("testparty-0", parties.get(0).getKey());

    parties = repository.findByLocationWithin(zone2LowerLeft, zone2UpperRight);

    assertEquals(12, parties.size()); //all the parties except the special one at 100, 100

    parties = repository.findByLocationWithin(zoneEmptyLowerLeft, zoneEmptyUpperRight);
    assertEquals(0, parties.size());
  }

  @Test
  public void testPolygonAndArrayOfPointsProduceSameResult() {
    Set<String> expectedKeys = new HashSet<String>();
    expectedKeys.add("testparty-2");
    expectedKeys.add("testparty-3");
    expectedKeys.add("testparty-4");
    expectedKeys.add("testparty-5");
    //a zone that engulfs parties 2, 3, 4 and 5, in the shape of a downward arrow pointing right
    Polygon zone = new Polygon(new Point(1, -2),
        new Point(3, -1.5),
        new Point(6, -4),
        new Point(5.5, -5.5),
        new Point(3, -5));
    Point[] points = zone.getPoints().toArray(new Point[5]);

    List<Party> fromZone = repository.findByLocationWithin(zone);
    List<Party> fromPoints = repository.findByLocationWithin(points);

    assertEquals(4, fromZone.size());
    assertEquals(fromZone, fromPoints);
    Set<String> keys = new HashSet<String>();
    for (Party party : fromZone) {
      keys.add(party.getKey());
    }
    assertEquals(expectedKeys, keys);
  }

  @Test
  public void testProvidingOnePointIsRejected() {
    try {
      repository.findByLocationWithin(new Point(0, 0));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Cannot compute a bounding box for within, 2 Point needed, missing parameter", e.getMessage());
    }

    try {
      repository.findByLocationWithin(new Point(0, 0), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Cannot compute a bounding box for within, 2 Point needed, got null", e.getMessage());
    }
  }

  @Test
  public void testProvidingOneJsonArrayIsRejected() {
    try {
      List<Party> parties = repository.findByLocationWithin(JsonArray.from(0,0));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("2 JsonArray required for within: startRange and endRange, missing parameter", e.getMessage());
    }
  }

  @Test(expected = CouchbaseQueryExecutionException.class)
  public void testJsonArrayWithNonNumericalValueProducesServerSideError() {
    repository.findByLocationWithin(JsonArray.from("toto", -2), JsonArray.from(4, 1));
    fail();
  }

  @Test
  public void testWithinJsonArrayRangesFiltersLocationAndAttendees() {
    List<Party> parties = repository.findByLocationWithin(JsonArray.from(0, -4, 115), JsonArray.from(4, 1, 132));
    assertEquals(2, parties.size());
  }

  @Test
  public void testDimensionalAnnotationCanBeUsedAsMeta() {
    try {
      //will trigger a specific message if parsed by SpatialViewQueryCreator
      repository.findByLocationIsWithin(new Point(0,0), null);
      fail("expected IllegalArgumentException from SpatialViewQueryCreator");
    } catch (IllegalArgumentException e) {
      assertEquals("Cannot compute a bounding box for within, 2 Point needed, got null", e.getMessage());
    }

    //when it is correctly formed, it actually returns data
    assertEquals(1, repository.findByLocationIsWithin(new Point(-10.5, -0.5), new Point(0.5, 10.5)).size());
  }

  @Test
  public void testFindWithinBoxCornerOrderDoesMatter() {
    Point a = new Point(0, -4);
    Point b = new Point(6, -2);
    Box box1 = new Box(a, b);
    Box box2 = new Box(b, a);

    final List<Party> parties1 = repository.findByLocationWithin(box1);
    final List<Party> parties2 = repository.findByLocationWithin(box2);

    assertEquals(3, parties1.size());
    assertNotEquals(parties1, parties2);
  }
}
