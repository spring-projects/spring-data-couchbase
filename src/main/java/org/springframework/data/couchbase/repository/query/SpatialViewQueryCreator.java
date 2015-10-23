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

package org.springframework.data.couchbase.repository.query;

import java.util.Iterator;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.view.SpatialViewQuery;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.query.Dimensional;
import org.springframework.data.couchbase.repository.query.support.GeoUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Shape;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * A QueryCreator that will enrich a {@link SpatialViewQuery} using query derivation mechanisms
 * and the parsed {@link PartTree}.
 * <p>
 * Support for query derivation keywords is limited, it is triggered by having a {@link Dimensional} annotation
 * on the query method.
 * <br/>
 * Here are the {@link Part.Type} supported:
 * <ul>
 *   <li><b>WITHIN:</b> finds elements contained in the provided {@link Shape}, {@link Point Point[] polygon},
 *   pair of {@link Point}s bounding box (lower left+upper right) or pair of raw {@link JsonArray} (discouraged as it
 *   leaks Couchbase specific class in your method signature, needs to be numerical data)</li>
 *   <li><b>NEAR:</b> finds elements near a provided {@link Point}, within the provided {@link Distance}</li>
 *   <li><b>GREATER_THAN, AFTER, GREATER_THAN_EQUALS</b>: adds a numerical element to the startRange and null to the endRange
 *   (useful for non geographic additional dimensions)</li>
 *   <li><b>LESS_THAN, BEFORE, LESS_THAN_EQUALS</b>: adds null to the startRange and a numerical element to the endRange
 *   (useful for non geographic additional dimensions)</li>
 *   <li><b>SIMPLE_PROPERTY (Is, Equals)</b>: adds a numerical element to both the startRange and the endRange
 *   (useful for non geographic additional dimensions)</li>
 *   <li><b>BETWEEN</b>: adds a numerical element to the startRange and a second numerical element to the endRange
 *   (useful for non geographic additional dimensions)</li>
 * </ul>
 * </p>
 * Additionally, {@link PartTree#isLimiting()} will trigger usage of {@link SpatialViewQuery#limit(int) limit}.
 */
public class SpatialViewQueryCreator extends AbstractQueryCreator<SpatialViewQuery, SpatialViewQuery> {

  private final SpatialViewQuery query;
  private final PartTree tree;
  private final CouchbaseConverter converter;

  private final int dimensions;
  private final JsonArray startRange;
  private final JsonArray endRange;

  public SpatialViewQueryCreator(int dimensions, PartTree tree, ParameterAccessor parameters, SpatialViewQuery query,
                                 CouchbaseConverter converter) {
    super(tree, parameters);
    this.query = query;
    this.tree = tree;
    this.converter = converter;
    this.dimensions = dimensions;
    this.startRange = JsonArray.create();
    this.endRange = JsonArray.create();
  }

  @Override
  protected SpatialViewQuery create(Part part, Iterator<Object> objectIterator) {
    ConvertingIterator iterator = new ConvertingIterator(objectIterator, converter);

    switch (part.getType()) {
      case WITHIN:
        applyWithin(startRange, endRange, iterator);
        break;
      case NEAR:
        applyNear(startRange, endRange, iterator);
        break;
      case GREATER_THAN:
      case GREATER_THAN_EQUAL:
      case AFTER:
        startRange.add(checkedNext(iterator, Object.class, null));
        endRange.addNull();
        break;
      case LESS_THAN:
      case LESS_THAN_EQUAL:
      case BEFORE:
        startRange.addNull();
        endRange.add(checkedNext(iterator, Object.class, null));
        break;
      case SIMPLE_PROPERTY:
        Object equals = checkedNext(iterator, Object.class, null);
        startRange.add(equals);
        endRange.add(equals);
        break;
      case BETWEEN:
        startRange.add(checkedNext(iterator, Object.class, null));
        endRange.add(checkedNext(iterator, Object.class, null));
        break;
      default:
        throw new IllegalArgumentException("Unsupported keyword in Spatial View query derivation: " + part.toString());
    }

    //will complete the ranges in complete step (if not enough elements for the ranges to match dimension count)
    return query;
  }

  private static void completeRangeIfNeeded(JsonArray range, int dimensions) {
    for (int i = range.size(); i < dimensions; i++) {
      range.addNull();
    }
  }

  private static void applyWithin(JsonArray startRange, JsonArray endRange, Iterator<Object> iterator) {
    if (!iterator.hasNext()) {
      throw new IllegalArgumentException("Not enough parameters for within");
    }

    Object next = iterator.next();
    if (next instanceof Shape) {
      GeoUtils.convertShapeTo2DRanges(startRange, endRange, (Shape) next);
    } else if (next instanceof Point) {
      //expect another point for the other corner of the bounding box
      Point northwest = (Point) next;
      Point southeast = checkedNext(iterator, Point.class, "Cannot compute a bounding box for within, 2 Point needed");
      GeoUtils.convertPointsTo2DRanges(startRange, endRange, true, northwest, southeast);
    } else if (next instanceof Point[]) {
      GeoUtils.convertPointsTo2DRanges(startRange, endRange, false, (Point[]) next);
    }  else if (next instanceof JsonArray) { //discouraged, leaks Couchbase classes into signatures
      JsonArray first = (JsonArray) next;
      for (Object o : first) {
        startRange.add(o);
      }

      JsonArray second = checkedNext(iterator, JsonArray.class, "2 JsonArray required for within: startRange and endRange");
      for (Object o : second) {
        endRange.add(o);
      }
    } else {
      //TODO Collection<Point>: same as polygon
      //TODO Collection<Double/Number>: one collection per range
      //TODO Number[]: one array per range
      throw new IllegalArgumentException("Unsupported parameter type for within: " + next.getClass());
    }
  }

  private static void applyNear(JsonArray startRange, JsonArray endRange, Iterator<Object> iterator) {
    if (!iterator.hasNext()) {
      throw new IllegalArgumentException("Not enough parameters for near");
    }

    Point near = checkedNext(iterator, Point.class, "Near queries need a Point as first argument");
    Distance distance = checkedNext(iterator, Distance.class, "Near queries need a maximum Distance as second argument");

    double[] boundingBox = GeoUtils.getBoundingBoxForNear(near, distance);

    startRange.add(boundingBox[0]).add(boundingBox[1]);
    endRange.add(boundingBox[2]).add(boundingBox[3]);
  }

  /**
   * Retrieve next value in the iterator assuming it is of the specified type (otherwise throw
   * an {@link IllegalArgumentException}.
   *
   * @param iterator the iterator to peek into.
   * @param clazz the expected type of the next value in the iterator.
   * @param errorMsg the error message prefix to use in the exception (will append a short message
   *   describing if the iterator has no value or if the type found was different than expected).
   * @param <T> the desired return type.
   * @return the next value as a T.
   * @throws IllegalArgumentException if there is no next value or it doesn't conform to the desired type.
   */
  private static <T> T checkedNext(Iterator<?> iterator, Class<T> clazz, String errorMsg) {
    if (errorMsg == null) {
      errorMsg = "Expected an additional parameter of type " + clazz.getName();
    }

    if (!iterator.hasNext()) {
      throw new IllegalArgumentException(errorMsg + ", missing parameter");
    }

    Object next = iterator.next();
    if (clazz.isInstance(next)) {
      return (T) next;
    } else if (next == null) {
      throw new IllegalArgumentException(errorMsg + ", got null");
    } else {
      throw new IllegalArgumentException(errorMsg + ", got a " + next.getClass().getName());
    }
  }

  @Override
  protected SpatialViewQuery and(Part part, SpatialViewQuery base, Iterator<Object> iterator) {
    return create(part, iterator);//and not really supported, all query derivation mutate the original ViewQuery
  }

  @Override
  protected SpatialViewQuery or(SpatialViewQuery base, SpatialViewQuery criteria) {
    //this won't be called unless there's a Or keyword in the method
    throw new UnsupportedOperationException("Or is not supported for View-based queries");
  }

  @Override
  protected SpatialViewQuery complete(SpatialViewQuery criteria, Sort sort) {
    if (sort != null) {
      throw new IllegalArgumentException("Sort is not supported on Spatial View queries");
    }

    if (tree.isLimiting()) {
      query.limit(tree.getMaxResults());
    }

    if (startRange.isEmpty() && endRange.isEmpty()) {
      return query;
    }

    completeRangeIfNeeded(startRange, dimensions);
    completeRangeIfNeeded(endRange, dimensions);
    return query.range(startRange, endRange);
  }
}
