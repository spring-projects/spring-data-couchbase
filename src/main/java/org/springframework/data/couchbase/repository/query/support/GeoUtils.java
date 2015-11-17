/*
 * Copyright 2012-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.query.support;

import com.couchbase.client.java.document.json.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.geo.Shape;

/**
 * Utility class to deal with geo/dimensional indexed data and queries.
 *
 * @author Simon Baslé
 */
public class GeoUtils {

  /**
   * Computes the bounding box approximation for a "near" query (max distance from a point of origin).
   *
   * @param origin the point of origin, center for the query.
   * @param distance the max distance to search within (negative distances will be multiplied by -1).
   * @return the bounding box approximation for this query ([xMin, yMin, xMax, yMax]).
   * @throws NullPointerException if any of the origin and distance are null
   */
  public static double[] getBoundingBoxForNear(Point origin, Distance distance) {
    if (origin == null || distance == null) throw new NullPointerException("Origin and distance required");

    //since maxDistance COULD be negative, we have to make sure we have correct min/max
    double maxDistance = Math.abs(distance.getNormalizedValue());
    double xMin = origin.getX() - maxDistance;
    double yMin = origin.getY() - maxDistance;
    double xMax = origin.getX() + maxDistance;
    double yMax = origin.getY() + maxDistance;

    return new double[] { xMin, yMin, xMax, yMax };
  }

  /**
   * Convert a sequence of {@link Point Points} describing a polygon to a pair of
   * {@link JsonArray} ranges corresponding to that polygon's bounding box,
   * and inject the coordinates into startRange and endRange.
   * If it is already equivalent to a Box (upper-left Point + lower-right Point), set
   * <i>isBoundingBox</i> to true.
   * Otherwise, this method will attempt to find the bounding box by finding the lowest
   * and highest X and Y coordinates.
   *
   * @param startRange the startRange to populate with this shape's data.
   * @param endRange the endRange to populate with this shape's data.
   * @param isBoundingBox true to skip finding min/max X and Y coordinates and use 2 Points as a {@link Box}.
   * @param points the sequence of Points.
   * @throws IllegalArgumentException if no points are provided, or in the case of isBoundingBox true
   *   if more or less than 2 points are provided or the 2 points are not ordered (a.x <= b.x && a.y <= b.y).
   */
  public static void convertPointsTo2DRanges(JsonArray startRange, JsonArray endRange, boolean isBoundingBox, Point... points) {
    if (points == null || points.length == 0) {
      throw new IllegalArgumentException("Needs points to convert");
    }

    if (isBoundingBox) {
      if (points.length != 2) {
        throw new IllegalArgumentException("Bounding box must be made of 2 points");
      }
      if (points[0].getX() > points[1].getX() || points[0].getY() > points[1].getY()) {
        throw new IllegalArgumentException("Bounding box must have point A on the lower left of point B");
      }
      startRange.add(points[0].getX()).add(points[0].getY());
      endRange.add(points[1].getX()).add(points[1].getY());
    } else {
      //this is like a polygon, find the bounding box
      //find the lowest and highest X and Y to get the bounding box
      double xMin = Double.POSITIVE_INFINITY;
      double yMin = Double.POSITIVE_INFINITY;
      double xMax = Double.NEGATIVE_INFINITY;
      double yMax = Double.NEGATIVE_INFINITY;
      for (Point point : points) {
        xMin = point.getX() < xMin ? point.getX() : xMin;
        xMax = point.getX() > xMax ? point.getX() : xMax;

        yMin = point.getY() < yMin ? point.getY() : yMin;
        yMax = point.getY() > yMax ? point.getY() : yMax;
      }

      //once we have the coordinates of lower left and upper right points, use them
      startRange.add(xMin).add(yMin);
      endRange.add(xMax).add(yMax);
    }
  }

  /**
   * Convert a {@link Shape} to a pair of {@link JsonArray} ranges, injected into startRange and endRange.
   *
   * @param startRange the startRange to populate with this shape's data.
   * @param endRange the endRange to populate with this shape's data.
   * @param shape the shape to extract ranges from.
   * @throws IllegalArgumentException if the {@link Shape} is unsupported.
   */
  public static void convertShapeTo2DRanges(JsonArray startRange, JsonArray endRange, Shape shape) {
    if (shape instanceof Box) {
      Box box = (Box) shape;
      startRange //add minimum coordinates for x and y
          .add(box.getFirst().getX())
          .add(box.getFirst().getY());
      endRange //add maximum coordinates for x and y
          .add(box.getSecond().getX())
          .add(box.getSecond().getY());
    } else if (shape instanceof Polygon) {
      //find the lowest and highest X and Y to get the bounding box
      double xMin = Double.POSITIVE_INFINITY;
      double yMin = Double.POSITIVE_INFINITY;
      double xMax = Double.NEGATIVE_INFINITY;
      double yMax = Double.NEGATIVE_INFINITY;
      for (Point point : (Polygon) shape) {
        xMin = point.getX() < xMin ? point.getX() : xMin;
        xMax = point.getX() > xMax ? point.getX() : xMax;

        yMin = point.getY() < yMin ? point.getY() : yMin;
        yMax = point.getY() > yMax ? point.getY() : yMax;
      }

      //once we have the coordinates of lower left and upper right points, use them
      startRange.add(xMin).add(yMin);
      endRange.add(xMax).add(yMax);
    } else if (shape instanceof Circle) {
      //here the bounding box is the box that contains the circle
      Circle circle = (Circle) shape;
      Point center = circle.getCenter();
      double radius = circle.getRadius().getNormalizedValue();

      double xMin = center.getX() - radius;
      double xMax = center.getX() + radius;
      double yMin = center.getY() - radius;
      double yMax = center.getY() + radius;

      startRange.add(xMin).add(yMin);
      endRange.add(xMax).add(yMax);
    } else {
      throw new IllegalArgumentException("Unsupported shape " + shape.getClass().getName());
    }
  }
}
