/*
 * Copyright 2012-2024 the original author or authors
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;

/**
 * PointInShapeEvaluator can be used to tell if a particular {@link Point} is contained by a {@link Polygon} or
 * {@link Circle}. It is most useful to eliminate false positive when a geo query has been made using a {@link Box
 * bounding box} approximation. For the purpose of this class, a point on the edge of a polygon isn't considered within
 * it. On the contrary a point on the edge of a circle is considered in it (distance from center <= radius). To that
 * end, {@link #removeFalsePositives(Collection, Converter, Circle) additional methods} that return a {@link List} of
 * objects with false positives removed are also provided. However, these need a {@link Converter Converter&lt;T,
 * Point&gt;} to extract the location attribute that should be tested against the polygon/circle.
 *
 * @author Simon Baslé
 * @see AwtPointInShapeEvaluator for a simple implementation based on AWT standard library.
 */
public abstract class PointInShapeEvaluator {

	/**
	 * Determine if a {@link Point} is contained by a {@link Polygon}.
	 *
	 * @param p the point to test.
	 * @param polygon the polygon we want the point to be in.
	 * @return true if the polygon contains the point, false otherwise.
	 */
	public abstract boolean pointInPolygon(Point p, Polygon polygon);

	/**
	 * Determine if a {@link Point} is contained by a polygon represented as an array of {@link Point points}. The points
	 * are not required to form a closed shape, but can (by having the first and last points be the same).
	 *
	 * @param p the point to test.
	 * @param points the Point[] representation of the polygon we want the point to be in.
	 * @return true if the polygon contains the point, false otherwise.
	 */
	public abstract boolean pointInPolygon(Point p, Point... points);

	/**
	 * Determine if a {@link Point} is contained by a {@link Circle}.
	 *
	 * @param p the point to test.
	 * @param c the Circle we want the point to be in.
	 * @return true if the circle contains the point, false otherwise.
	 */
	public abstract boolean pointInCircle(Point p, Circle c);

	/**
	 * Determine if a {@link Point} is contained by a {@link Circle} represented by its {@link Point center Point} and
	 * {@link Distance Distance radius}.
	 *
	 * @param p the point to test.
	 * @param center the center Point of the Circle we want the point to be in.
	 * @param radius the Distance radius of the Circle we want the point to be in.
	 * @return true if the circle contains the point, false otherwise.
	 */
	public abstract boolean pointInCircle(Point p, Point center, Distance radius);

	/**
	 * Utility method to remove false positives from a collection of objects that have a notion of location, where we want
	 * to only include values that are located within a polygon. The collection should usually be already contained in the
	 * bounding box approximation of the polygon for maximum efficiency.
	 *
	 * @param boundingBoxResults the collections of located objects approximately inside the target polygon.
	 * @param locationExtractor a {@link Converter} to extract the location of the value objects.
	 * @param polygon the target polygon.
	 * @param <T> the type of located value objects in the collection.
	 * @return a {@link List} of the value objects which location has been verified to actually be contained within the
	 *         polygon.
	 */
	public <T> List<T> removeFalsePositives(Collection<? extends T> boundingBoxResults,
			Converter<T, Point> locationExtractor, Polygon polygon) {
		ArrayList<T> result = new ArrayList<T>(boundingBoxResults.size());
		for (T boxResult : boundingBoxResults) {
			Point p = locationExtractor.convert(boxResult);
			if (pointInPolygon(p, polygon)) {
				result.add(boxResult);
			}
		}
		result.trimToSize();
		return result;
	}

	/**
	 * Utility method to remove false positives from a collection of objects that have a notion of location, where we want
	 * to only include values that are located within a circle. The collection should usually be already contained in the
	 * bounding box approximation of the circle for maximum efficiency.
	 *
	 * @param boundingBoxResults the collections of located objects approximately inside the target circle.
	 * @param locationExtractor a {@link Converter} to extract the location of the value objects.
	 * @param circle the target circle.
	 * @param <T> the type of located value objects in the collection.
	 * @return a {@link List} of the value objects which location has been verified to actually be contained within the
	 *         circle.
	 */
	public <T> List<T> removeFalsePositives(Collection<? extends T> boundingBoxResults,
			Converter<T, Point> locationExtractor, Circle circle) {
		ArrayList<T> result = new ArrayList<T>(boundingBoxResults.size());
		for (T boxResult : boundingBoxResults) {
			Point p = locationExtractor.convert(boxResult);
			if (pointInCircle(p, circle)) {
				result.add(boxResult);
			}
		}
		result.trimToSize();
		return result;
	}

	/**
	 * Utility method to remove false positives from a collection of objects that have a notion of location, where we want
	 * to only include values that are located within a polygon. The collection should usually be already contained in the
	 * bounding box approximation of the polygon for maximum efficiency.
	 *
	 * @param boundingBoxResults the collections of located objects approximately inside the target polygon.
	 * @param locationExtractor a {@link Converter} to extract the location of the value objects.
	 * @param polygon the target polygon, as an array of {@link Point} (not necessarily closed).
	 * @param <T> the type of located value objects in the collection.
	 * @return a {@link List} of the value objects which location has been verified to actually be contained within the
	 *         polygon.
	 */
	public <T> List<T> removeFalsePositives(Collection<? extends T> boundingBoxResults,
			Converter<T, Point> locationExtractor, Point... polygon) {
		ArrayList<T> result = new ArrayList<T>(boundingBoxResults.size());
		for (T boxResult : boundingBoxResults) {
			Point p = locationExtractor.convert(boxResult);
			if (pointInPolygon(p, polygon)) {
				result.add(boxResult);
			}
		}
		result.trimToSize();
		return result;
	}

	/**
	 * Utility method to remove false positives from a collection of objects that have a notion of location, where we want
	 * to only include values that are located within a circle. The collection should usually be already contained in the
	 * bounding box approximation of the circle for maximum efficiency.
	 *
	 * @param boundingBoxResults the collections of located objects approximately inside the target circle.
	 * @param locationExtractor a {@link Converter} to extract the location of the value objects.
	 * @param center the center of the target circle.
	 * @param radius the radius of the target circle.
	 * @param <T> the type of located value objects in the collection.
	 * @return a {@link List} of the value objects which location has been verified to actually be contained within the
	 *         circle.
	 */
	public <T> List<T> removeFalsePositives(Collection<? extends T> boundingBoxResults,
			Converter<T, Point> locationExtractor, Point center, Distance radius) {
		ArrayList<T> result = new ArrayList<T>(boundingBoxResults.size());
		for (T boxResult : boundingBoxResults) {
			Point p = locationExtractor.convert(boxResult);
			if (pointInCircle(p, center, radius)) {
				result.add(boxResult);
			}
		}
		result.trimToSize();
		return result;
	}
}
