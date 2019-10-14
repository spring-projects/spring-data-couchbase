package org.springframework.data.couchbase.repository.query.support;

import org.junit.Test;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.geo.Shape;

import com.couchbase.client.java.document.json.JsonArray;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/**
 * Unit tests for the {@link GeoUtils} utility class.
 * @author Simon Baslé
 */
public class GeoUtilsTest {

  @Test
  public void testGetBoundingBoxForNear() throws Exception {
    final Distance distance = new Distance(120);
    double[] bbox = GeoUtils.getBoundingBoxForNear(new Point(1, 1), distance);
    assertThat(bbox[0]).isCloseTo(-119d, offset(0d)); //xMin
    assertThat(bbox[2]).isCloseTo(121d, offset(0d)); //xMax
    assertThat(bbox[1]).isCloseTo(-119d, offset(0d)); //yMin
    assertThat(bbox[3]).isCloseTo(121d, offset(0d)); //yMax

    bbox = GeoUtils.getBoundingBoxForNear(new Point(-3,-5), distance);
    assertThat(bbox[0]).isCloseTo(-123d, offset(0d)); //xMin
    assertThat(bbox[2]).isCloseTo(117d, offset(0d)); //xMax
    assertThat(bbox[1]).isCloseTo(-125d, offset(0d)); //yMin
    assertThat(bbox[3]).isCloseTo(115d, offset(0d)); //yMax
  }

  @Test
  public void testGetBoundingBoxForNearNegativeDistance() throws Exception {
    final Distance distance = new Distance(-120);
    double[] bbox = GeoUtils.getBoundingBoxForNear(new Point(1, 1), distance);
    assertThat(bbox[0]).isCloseTo(-119d, offset(0d)); //xMin
    assertThat(bbox[2]).isCloseTo(121d, offset(0d)); //xMax
    assertThat(bbox[1]).isCloseTo(-119d, offset(0d)); //yMin
    assertThat(bbox[3]).isCloseTo(121d, offset(0d)); //yMax

    bbox = GeoUtils.getBoundingBoxForNear(new Point(-3,-5), distance);
    assertThat(bbox[0]).isCloseTo(-123d, offset(0d)); //xMin
    assertThat(bbox[2]).isCloseTo(117d, offset(0d)); //xMax
    assertThat(bbox[1]).isCloseTo(-125d, offset(0d)); //yMin
    assertThat(bbox[3]).isCloseTo(115d, offset(0d)); //yMax
  }

  @Test(expected = NullPointerException.class)
  public void testGetBoundingBoxForNearNullOrigin() {
    GeoUtils.getBoundingBoxForNear(null, new Distance(100));
  }

  @Test(expected = NullPointerException.class)
  public void testGetBoundingBoxForNearNullDistance() {
    GeoUtils.getBoundingBoxForNear(new Point(1, 1), null);
  }

  @Test(expected = NullPointerException.class)
  public void testGetBoundingBoxForNearNullOriginAndDistance() {
    GeoUtils.getBoundingBoxForNear(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertPointsTo2DRangesIsBoundingBoxFailsIfLessThan2Points() {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();
    Point p1 = new Point(2,3);

    GeoUtils.convertPointsTo2DRanges(startRange, endRange, true, p1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertPointsTo2DRangesIsBoundingBoxFailsIfMoreThan2Points() {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();
    Point p1 = new Point(2,3);
    Point p2 = new Point(4,5);
    Point p3 = new Point(6,7);

    GeoUtils.convertPointsTo2DRanges(startRange, endRange, true, p1, p2, p3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertPointsTo2DRangesIsBoundingBoxFailsIfNotOrderedPoints() {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();
    Point p1 = new Point(2,3);
    Point p2 = new Point(4,5);

    GeoUtils.convertPointsTo2DRanges(startRange, endRange, true, p2, p1);
  }

  @Test
  public void testConvertPointsTo2DRanges2PointsBoundingBox() throws Exception {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();
    Point p1 = new Point(2,3);
    Point p2 = new Point(4,5);

    GeoUtils.convertPointsTo2DRanges(startRange, endRange, true, p1, p2);

    assertThat(startRange.size()).isEqualTo(2);
    assertThat(endRange.size()).isEqualTo(2);
    assertThat(startRange.getDouble(0)).isCloseTo(2d, offset(0d));
    assertThat(startRange.getDouble(1)).isCloseTo(3d, offset(0d));
    assertThat(endRange.getDouble(0)).isCloseTo(4d, offset(0d));
    assertThat(endRange.getDouble(1)).isCloseTo(5d, offset(0d));
  }

  @Test
  public void testConvertPointsTo2DRanges2PointsPoly() throws Exception {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();
    Point p1 = new Point(2,3);
    Point p2 = new Point(4,5);

    GeoUtils.convertPointsTo2DRanges(startRange, endRange, false, p1, p2);

    assertThat(startRange.size()).isEqualTo(2);
    assertThat(endRange.size()).isEqualTo(2);
    assertThat(startRange.getDouble(0)).isCloseTo(2d, offset(0d));
    assertThat(startRange.getDouble(1)).isCloseTo(3d, offset(0d));
    assertThat(endRange.getDouble(0)).isCloseTo(4d, offset(0d));
    assertThat(endRange.getDouble(1)).isCloseTo(5d, offset(0d));
  }

  @Test
  public void testConvertPointsTo2DRanges3PointsPoly() throws Exception {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();
    Point p1 = new Point(2,3);
    Point p2 = new Point(-4, 3);
    Point p3 = new Point(6, -12);

    GeoUtils.convertPointsTo2DRanges(startRange, endRange, false, p1, p2, p3);

    assertThat(startRange.size()).isEqualTo(2);
    assertThat(endRange.size()).isEqualTo(2);
    assertThat(startRange.getDouble(0)).isCloseTo(-4d, offset(0d));
    assertThat(startRange.getDouble(1)).isCloseTo(-12d, offset(0d));
    assertThat(endRange.getDouble(0)).isCloseTo(6d, offset(0d));
    assertThat(endRange.getDouble(1)).isCloseTo(3d, offset(0d));
  }

  @Test
  public void testConvertPointsTo2DRanges8Points() throws Exception {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();
    //bounding box of (3,3)(9,9)
    GeoUtils.convertPointsTo2DRanges(startRange, endRange, false,
        new Point(3, 3),
        new Point(3, 7),
        new Point(6, 7),
        new Point(6, 9),
        new Point(9, 9),
        new Point(9, 5),
        new Point(6, 5),
        new Point(6, 3));

    assertThat(startRange.size()).isEqualTo(2);
    assertThat(endRange.size()).isEqualTo(2);
    assertThat(startRange.getDouble(0)).isCloseTo(3d, offset(0d));
    assertThat(startRange.getDouble(1)).isCloseTo(3d, offset(0d));
    assertThat(endRange.getDouble(0)).isCloseTo(9d, offset(0d));
    assertThat(endRange.getDouble(1)).isCloseTo(9d, offset(0d));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertPointsTo2DRangesNullPoints() throws Exception {
    GeoUtils.convertPointsTo2DRanges(JsonArray.empty(), JsonArray.empty(), false, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertPointsTo2DRangesEmptyPoints() throws Exception {
    GeoUtils.convertPointsTo2DRanges(JsonArray.empty(), JsonArray.empty(), false, new Point[0]);
  }

  @Test
  public void testConvertShapeTo2DRangesBox() throws Exception {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();
    Box box = new Box(new Point(0, 5), new Point(10, 30));

    GeoUtils.convertShapeTo2DRanges(startRange, endRange, box);

    assertThat(startRange.size()).isEqualTo(2);
    assertThat(endRange.size()).isEqualTo(2);
    assertThat(startRange.getDouble(0)).isCloseTo(0d, offset(0d));
    assertThat(startRange.getDouble(1)).isCloseTo(5d, offset(0d));
    assertThat(endRange.getDouble(0)).isCloseTo(10d, offset(0d));
    assertThat(endRange.getDouble(1)).isCloseTo(30, offset(0d));
  }

  @Test
  public void testConvertShapeTo2DRangesBoxIsNotReordered() throws Exception {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();
    Box box = new Box(new Point(0, 5), new Point(10, -3));

    GeoUtils.convertShapeTo2DRanges(startRange, endRange, box);

    assertThat(startRange.size()).isEqualTo(2);
    assertThat(endRange.size()).isEqualTo(2);
    assertThat(startRange.getDouble(0)).isCloseTo(0d, offset(0d));
    assertThat(startRange.getDouble(1)).isCloseTo(5d, offset(0d));
    assertThat(endRange.getDouble(0)).isCloseTo(10d, offset(0d));
    assertThat(endRange.getDouble(1)).isCloseTo(-3d, offset(0d));
  }

  @Test
  public void testConvertPolygonBoxTo2DRangesIsReordered() throws Exception {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();
    Polygon box = new Polygon(new Point(0, 5), new Point(10, -3), new Point(0, 5));

    GeoUtils.convertShapeTo2DRanges(startRange, endRange, box);

    assertThat(startRange.size()).isEqualTo(2);
    assertThat(endRange.size()).isEqualTo(2);
    assertThat(startRange.getDouble(0)).isCloseTo(0d, offset(0d));
    assertThat(startRange.getDouble(1)).isCloseTo(-3d, offset(0d));
    assertThat(endRange.getDouble(0)).isCloseTo(10d, offset(0d));
    assertThat(endRange.getDouble(1)).isCloseTo(5d, offset(0d));
  }

  @Test
  public void testConvertShapeTo2DRangesPolygon() throws Exception {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();
    Polygon polygon = new Polygon(
        new Point(3, 3),
        new Point(3, 7),
        new Point(6, 7),
        new Point(6, 9),
        new Point(9, 9),
        new Point(9, 5),
        new Point(6, 5),
        new Point(6, 3)
    );

    GeoUtils.convertShapeTo2DRanges(startRange, endRange, polygon);

    assertThat(startRange.size()).isEqualTo(2);
    assertThat(endRange.size()).isEqualTo(2);
    assertThat(startRange.getDouble(0)).isCloseTo(3d, offset(0d));
    assertThat(startRange.getDouble(1)).isCloseTo(3d, offset(0d));
    assertThat(endRange.getDouble(0)).isCloseTo(9d, offset(0d));
    assertThat(endRange.getDouble(1)).isCloseTo(9d, offset(0d));
  }

  @Test
  public void testConvertShapeTo2DRangesPolygonIsSameAsMultiplePointsTo2DRanges() throws Exception {
    JsonArray startRangePoints = JsonArray.create();
    JsonArray endRangePoints = JsonArray.create();
    JsonArray startRangePolygon = JsonArray.create();
    JsonArray endRangePolygon = JsonArray.create();

    GeoUtils.convertPointsTo2DRanges(startRangePoints, endRangePoints, false,
        new Point(3, 3),
        new Point(3, 7),
        new Point(6, 7),
        new Point(6, 9),
        new Point(9, 9),
        new Point(9, 5),
        new Point(6, 5),
        new Point(6, 3));

    Polygon polygon = new Polygon(
        new Point(3, 3),
        new Point(3, 7),
        new Point(6, 7),
        new Point(6, 9),
        new Point(9, 9),
        new Point(9, 5),
        new Point(6, 5),
        new Point(6, 3));

    GeoUtils.convertShapeTo2DRanges(startRangePolygon, endRangePolygon, polygon);

    assertThat(startRangePoints.size()).isEqualTo(2);
    assertThat(endRangePoints.size()).isEqualTo(2);
    assertThat(startRangePoints.getDouble(0)).isCloseTo(3d, offset(0d));
    assertThat(startRangePoints.getDouble(1)).isCloseTo(3d, offset(0d));
    assertThat(endRangePoints.getDouble(0)).isCloseTo(9d, offset(0d));
    assertThat(endRangePoints.getDouble(1)).isCloseTo(9d, offset(0d));
    assertThat(endRangePolygon).isEqualTo(endRangePoints);
    assertThat(startRangePolygon).isEqualTo(startRangePoints);
  }

  @Test
  public void testConvertShapeTo2DRangesCircle() throws Exception {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();
    Circle circle = new Circle(new Point(0, 0), new Distance(3));

    GeoUtils.convertShapeTo2DRanges(startRange, endRange, circle);

    assertThat(startRange.size()).isEqualTo(2);
    assertThat(endRange.size()).isEqualTo(2);
    assertThat(startRange.getDouble(0)).isCloseTo(-3d, offset(0d));
    assertThat(startRange.getDouble(1)).isCloseTo(-3d, offset(0d));
    assertThat(endRange.getDouble(0)).isCloseTo(3d, offset(0d));
    assertThat(endRange.getDouble(1)).isCloseTo(3d, offset(0d));
  }

  @Test
  public void testCircleApproximationIsSameAsNearApproximation() throws Exception {
    JsonArray startRangeCircle = JsonArray.create();
    JsonArray endRangeCircle = JsonArray.create();
    Point origin = new Point(0, 0);
    Distance distance = new Distance(3);
    Circle circle = new Circle(origin, distance);

    GeoUtils.convertShapeTo2DRanges(startRangeCircle, endRangeCircle, circle);
    double[] bbox = GeoUtils.getBoundingBoxForNear(origin, distance);

    assertThat(startRangeCircle.size()).isEqualTo(2);
    assertThat(endRangeCircle.size()).isEqualTo(2);

    assertThat(bbox[0]).isCloseTo(-3d, offset(0d));
    assertThat(bbox[1]).isCloseTo(-3d, offset(0d));
    assertThat(bbox[2]).isCloseTo(3d, offset(0d));
    assertThat(bbox[3]).isCloseTo(3d, offset(0d));

    assertThat(startRangeCircle.getDouble(0)).isCloseTo(bbox[0], offset(0d));
    assertThat(startRangeCircle.getDouble(1)).isCloseTo(bbox[1], offset(0d));
    assertThat(endRangeCircle.getDouble(0)).isCloseTo(bbox[2], offset(0d));
    assertThat(endRangeCircle.getDouble(1)).isCloseTo(bbox[3], offset(0d));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertShapeTo2DRangesOtherShape() throws Exception {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();

    Shape customShape = new Shape() {};

    GeoUtils.convertShapeTo2DRanges(startRange, endRange, customShape);
  }

  @Test(expected = NullPointerException.class)
  public void testConvertShapeTo2DRangesNullShape() throws Exception {
    JsonArray startRange = JsonArray.create();
    JsonArray endRange = JsonArray.create();

    GeoUtils.convertShapeTo2DRanges(startRange, endRange, null);
  }
}
