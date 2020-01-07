package org.springframework.data.couchbase.repository.query.support;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;

/**
 * An abstract base for testing {@link PointInShapeEvaluator} implementations.
 * Each implementation can be commonly tested by extending this base and instantiating the evaluator in createEvaluator.
 *
 * @author Simon Baslé
 */
public abstract class AbstractPointInShapeEvaluatorTest {

  public abstract PointInShapeEvaluator createEvaluator();

  private PointInShapeEvaluator evaluator;

  protected static final class LocatedValue {
    public final Point location;
    public final String name;

    public LocatedValue(Point location, String name) {
      this.location = location;
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  protected static final Converter<LocatedValue, Point> LOCATED_VALUE_POINT_CONVERTER = new Converter<LocatedValue, Point>() {
    @Override
    public Point convert(LocatedValue source) {
      return source.location;
    }
  };

  @Before
  public void init() {
    evaluator = createEvaluator();
  }

  @Test
  public void testPointInPolygon() throws Exception {
    Polygon openTriangle = new Polygon(
        new Point(1.0, 2.0),
        new Point(2.0, 2.0),
        new Point(1.5, 0.0)
    );

    Polygon closedTriangle = new Polygon(
        new Point(1.0, 2.0),
        new Point(2.0, 2.0),
        new Point(1.5, 0.0),
        new Point(1.0, 2.0) //explicitly closing the shape
    );

    Point inside = new Point(1.6, 1.8);
    Point outside = new Point(1.1, 0.3);
    Point edge = new Point(1.0, 2.0);

    assertTrue("point inside open polygon failed", evaluator.pointInPolygon(inside, openTriangle));
    assertFalse("point outside open polygon failed", evaluator.pointInPolygon(outside, openTriangle));
    assertFalse("point on edge of open polygon should not be considered within the polygon",
        evaluator.pointInPolygon(edge, openTriangle));
    assertTrue("point inside closed polygon failed", evaluator.pointInPolygon(inside, closedTriangle));
    assertFalse("point outside closed polygon failed", evaluator.pointInPolygon(outside, closedTriangle));
    assertFalse("point on edge of closed polygon should not be considered within the polygon",
        evaluator.pointInPolygon(edge, closedTriangle));
  }

  @Test
  public void testPointInPolygonArray() throws Exception {
    Point[] openTriangle = new Point[] {
        new Point(1.0, 2.0),
        new Point(2.0, 2.0),
        new Point(1.5, 0.0)
    };

    Point[] closedTriangle = new Point[] {
        new Point(1.0, 2.0),
        new Point(2.0, 2.0),
        new Point(1.5, 0.0),
        new Point(1.0, 2.0) //explicitly closing the shape
    };

    Point inside = new Point(1.6, 1.8);
    Point outside = new Point(1.1, 0.3);
    Point edge = new Point(1.0, 2.0);

    assertTrue("point inside open polygon failed", evaluator.pointInPolygon(inside, openTriangle));
    assertFalse("point outside open polygon failed", evaluator.pointInPolygon(outside, openTriangle));
    assertFalse("point on edge of open polygon should not be considered within the polygon",
        evaluator.pointInPolygon(edge, openTriangle));
    assertTrue("point inside closed polygon failed", evaluator.pointInPolygon(inside, closedTriangle));
    assertFalse("point outside closed polygon failed", evaluator.pointInPolygon(outside, closedTriangle));
    assertFalse("point on edge of closed polygon should not be considered within the polygon",
        evaluator.pointInPolygon(edge, closedTriangle));
  }

  @Test
  public void testPointInCircle() throws Exception {
    Circle circle = new Circle(new Point(0, 0), new Distance(2));

    Point inside = new Point(-0.3, 1.4);
    Point outside = new Point(1.3, 2d);
    Point onEdge = new Point(-2d, 0d);

    assertTrue("point inside failed", evaluator.pointInCircle(inside, circle));
    assertFalse("point outside failed", evaluator.pointInCircle(outside, circle));
    assertTrue("point on edge of circle should be considered within / near", evaluator.pointInCircle(onEdge, circle));
  }

  @Test
  public void testPointInCircleCenterRadius() throws Exception {
    Point center = new Point(0, 0);
    Distance radius = new Distance(2);

    Point inside = new Point(-0.3, 1.8);
    Point outside = new Point(1.3, 2d);
    Point onEdge = new Point(-2d, 0d);

    assertTrue("point inside failed", evaluator.pointInCircle(inside, center, radius));
    assertFalse("point outside failed", evaluator.pointInCircle(outside, center, radius));
    assertTrue("point on edge of circle should be considered within / near", evaluator.pointInCircle(onEdge, center, radius));
  }

  @Test
  public void testRemoveFalsePositivesPolygon() throws Exception {
    Polygon openTriangle = new Polygon(
        new Point(1.0, 2.0),
        new Point(2.0, 2.0),
        new Point(1.5, 0.0)
    );

    Polygon closedTriangle = new Polygon(
        new Point(1.0, 2.0),
        new Point(2.0, 2.0),
        new Point(1.5, 0.0),
        new Point(1.0, 2.0) //explicitly closing the shape
    );

    LocatedValue inside = new LocatedValue(new Point(1.6, 1.8), "inside");
    LocatedValue outside = new LocatedValue(new Point(1.1, 0.3), "outside");
    LocatedValue edge = new LocatedValue(new Point(1.5, 0d), "edge");

    List<LocatedValue> expected = Collections.singletonList(inside);
    List<LocatedValue> tested = Arrays.asList(inside, outside, edge);

    List<LocatedValue> filteredOpen = evaluator.removeFalsePositives(tested, LOCATED_VALUE_POINT_CONVERTER, openTriangle);
    List<LocatedValue> filteredClosed = evaluator.removeFalsePositives(tested, LOCATED_VALUE_POINT_CONVERTER, closedTriangle);

    assertEquals(expected, filteredOpen);
    assertEquals(expected, filteredClosed);
  }

  @Test
  public void testRemoveFalsePositivesPolygonArray() throws Exception {
    Point[] openTriangle = new Point[] {
        new Point(1.0, 2.0),
        new Point(2.0, 2.0),
        new Point(1.5, 0.0)
    };

    Point[] closedTriangle = new Point[] {
        new Point(1.0, 2.0),
        new Point(2.0, 2.0),
        new Point(1.5, 0.0),
        new Point(1.0, 2.0) //explicitly closing the shape
    };

    LocatedValue inside = new LocatedValue(new Point(1.6, 1.8), "inside");
    LocatedValue outside = new LocatedValue(new Point(1.1, 0.3), "outside");
    LocatedValue edge = new LocatedValue(new Point(1.5, 0d), "edge");

    List<LocatedValue> expected = Collections.singletonList(inside);
    List<LocatedValue> tested = Arrays.asList(inside, outside, edge);

    List<LocatedValue> filteredOpen = evaluator.removeFalsePositives(tested, LOCATED_VALUE_POINT_CONVERTER, openTriangle);
    List<LocatedValue> filteredClosed = evaluator.removeFalsePositives(tested, LOCATED_VALUE_POINT_CONVERTER, closedTriangle);

    assertEquals(expected, filteredOpen);
    assertEquals(expected, filteredClosed);
  }

  @Test
  public void testRemoveFalsePositivesCircle() throws Exception {
    Circle circle = new Circle(new Point(0, 0), new Distance(2));

    LocatedValue inside = new LocatedValue(new Point(-0.3, 1.4), "inside");
    LocatedValue outside = new LocatedValue(new Point(1.3, 2d), "outside");
    LocatedValue edge = new LocatedValue(new Point(-2d, 0d), "edge");

    List<LocatedValue> expected = Arrays.asList(inside, edge);
    List<LocatedValue> tested = Arrays.asList(inside, outside, edge);

    List<LocatedValue> filtered = evaluator.removeFalsePositives(tested, LOCATED_VALUE_POINT_CONVERTER, circle);

    assertEquals(expected, filtered);
  }

  @Test
  public void testRemoveFalsePositivesCircleCenterRadius() throws Exception {
    Point center = new Point(0, 0);
    Distance radius = new Distance(2);

    LocatedValue inside = new LocatedValue(new Point(-0.3, 1.4), "inside");
    LocatedValue outside = new LocatedValue(new Point(1.3, 2d), "outside");
    LocatedValue edge = new LocatedValue(new Point(-2d, 0d), "edge");

    List<LocatedValue> expected = Arrays.asList(inside, edge);
    List<LocatedValue> tested = Arrays.asList(inside, outside, edge);

    List<LocatedValue> filtered = evaluator.removeFalsePositives(tested, LOCATED_VALUE_POINT_CONVERTER, center, radius);

    assertEquals(expected, filtered);
  }
}
