/*
 * Copyright 2012-present the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.query.support;

import java.awt.geom.Path2D;
import java.awt.geom.Point2D;
import java.util.List;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;

/**
 * A default {@link PointInShapeEvaluator} implementation based on the JDK's java.awt.geom classes.
 *
 * @author Simon Baslé
 */
public class AwtPointInShapeEvaluator extends PointInShapeEvaluator {

	@Override
	public boolean pointInPolygon(Point p, Polygon polygon) {
		final List<Point> points = polygon.getPoints();
		Path2D awtPolygon = new Path2D.Double(Path2D.WIND_EVEN_ODD, points.size());
		boolean first = true;
		for (Point point : points) {
			if (first) {
				first = false;
				awtPolygon.moveTo(point.getX(), point.getY());
			} else {
				awtPolygon.lineTo(point.getX(), point.getY());
			}
		}
		awtPolygon.closePath();
		return pointInPolygon(p, awtPolygon);
	}

	@Override
	public boolean pointInPolygon(Point p, Point... points) {
		if (points == null)
			throw new NullPointerException("Polygon must at least contain 3 points");
		if (points.length < 3)
			throw new IllegalArgumentException("Polygon must at least contain 3 points");
		Path2D awtPolygon = new Path2D.Double(Path2D.WIND_EVEN_ODD, points.length);
		boolean first = true;
		for (Point point : points) {
			if (first) {
				first = false;
				awtPolygon.moveTo(point.getX(), point.getY());
			} else {
				awtPolygon.lineTo(point.getX(), point.getY());
			}
		}
		awtPolygon.closePath();
		return pointInPolygon(p, awtPolygon);
	}

	@Override
	public boolean pointInCircle(Point p, Circle c) {
		Point2D center = new Point2D.Double(c.getCenter().getX(), c.getCenter().getY());
		return pointNear(p, center, c.getRadius().getNormalizedValue());
	}

	@Override
	public boolean pointInCircle(Point p, Point center, Distance radiusDistance) {
		double radius = radiusDistance.getNormalizedValue();
		return pointNear(p, new Point2D.Double(center.getX(), center.getY()), radius);
	}

	private boolean pointInPolygon(Point p, Path2D awtPolygon) {
		return awtPolygon.contains(p.getX(), p.getY());
	}

	private boolean pointNear(Point p, Point2D center, double distance) {
		return center.distance(p.getX(), p.getY()) <= distance;
	}
}
