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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.ViewQuery;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * A QueryCreator that will enrich a {@link ViewQuery} using query derivation mechanisms
 * and the parsed {@link PartTree}.
 * <p>
 * Support for query derivation keywords is very limited (and especially you have to use one valid entity property name
 * in your query naming, and compound key views are not supported).
 * <br/>
 * Here are the {@link Part.Type} supported:
 * <ul>
 *   <li><b>GREATER_THAN_EQUAL:</b> uses {@link ViewQuery#startKey(String) startkey}</li>
 *   <li><b>LESS_THAN_EQUAL:</b> uses {@link ViewQuery#endKey(String) endkey} {@link ViewQuery#inclusiveEnd() inclusive}</li>
 *   <li><b>LESS_THAN / BEFORE:</b> uses {@link ViewQuery#endKey(String) endkey}</li>
 *   <li><b>BETWEEN:</b> uses {@link ViewQuery#startKey(String) startkey},
 *   {@link ViewQuery#endKey(String) endkey(exclusive)}</li>
 *   <li><b>STARTING_WITH:</b> (only with String key). uses {@link ViewQuery#startKey(String) startkey},
 *   {@link ViewQuery#endKey(String) endkey(exclusive)}. Will append special unicode char <code>\uefff</code>.</li>
 *   <li><b>SIMPLE_PROPERTY:</b> (aka "Is", "Equals"). This one can have no argument if used alone
 *   (eg. "findAllByUsername"), otherwise uses {@link ViewQuery#key(String) key}</li>
 *   <li><b>IN:</b> uses {@link ViewQuery#keys(JsonArray) keys} (provide a collection or array)</li>
 * </ul>
 * </p>
 * Additionally, {@link PartTree#isLimiting()} will use {@link ViewQuery#limit(int) limit}
 * and either {@link View#reduce()} or {@link PartTree#isCountProjection()} will trigger a {@link ViewQuery#reduce() reduce}.
 */
public class ViewQueryCreator extends AbstractQueryCreator<ViewQueryCreator.DerivedViewQuery, ViewQuery> {

  private ViewQuery query;
  private final View viewAnnotation;
  private final PartTree tree;
  private final int treeCount;
  private final CouchbaseConverter converter;

  public ViewQueryCreator(PartTree tree, ParameterAccessor parameters, View viewAnnotation,
                          ViewQuery query, CouchbaseConverter converter) {
    super(tree, parameters);
    this.query = query;
    this.viewAnnotation = viewAnnotation;
    this.tree = tree;
    this.converter = converter;

    //sanity check the partTree since we have strong restrictions on what's supported:
    int i = 0;
    Set<String> properties = new HashSet<String>();
    for (PartTree.OrPart parts : tree) {
      for (Part part : parts) {
        i++;
        properties.add(part.getProperty().toDotPath());
      }
    }
    this.treeCount = i;
    if (properties.size() > 1) {
      throw new IllegalArgumentException("View-based queries do not support compound keys");
    }
  }

  @Override
  protected ViewQuery create(Part part, Iterator<Object> objectIterator) {
    ConvertingIterator iterator = new ConvertingIterator(objectIterator, converter);

    switch (part.getType()) {
      case GREATER_THAN_EQUAL:
        startKey(iterator);
        break;
      case LESS_THAN_EQUAL:
        query.inclusiveEnd(true);
      case BEFORE:
      case LESS_THAN://fall-through on purpose here
        endKey(iterator);
        break;
      case BETWEEN:
        startKey(iterator);
        endKey(iterator);
        break;
      case STARTING_WITH: //starting_with only supports String keys
        String nameStart = nextString(iterator);
        query.startKey(nameStart).endKey(nameStart + "\uefff");
        query.inclusiveEnd(false);
        break;
      case SIMPLE_PROPERTY:
        key(iterator);
        break;
      case IN:
        query.keys(in(iterator));
        break;
      default:
        throw new IllegalArgumentException("Unsupported keyword in View query derivation: " + part.toString());
    }
    return query;
  }

  private void startKey(Iterator<Object> iterator) {
    if (!iterator.hasNext()) {
      throw new IllegalArgumentException("Not enough parameters for startKey");
    }

    Object next = iterator.next();
    if (next instanceof String) {
      query.startKey((String) next);
    } else if (next instanceof Boolean) {
      query.startKey((Boolean) next);
    } else if (next instanceof Double) {
      query.startKey((Double) next);
    } else if (next instanceof Integer) {
      query.startKey((Integer) next);
    } else if (next instanceof Long) {
      query.startKey((Long) next);
    } else if (next instanceof Collection) {
      //when creating a JsonArray, the from(List) method is preferred because it will convert internal
      //Lists and Maps to JsonObject and JsonArray respectively
      List<Object> arrayContent = new ArrayList<Object>((Collection) next);
      query.startKey(JsonArray.from(arrayContent));
    } else if (next.getClass().isArray()) {
      List<Object> arrayContent = Arrays.asList((Object[]) next);
      query.startKey(JsonArray.from(arrayContent));
    } else if (next instanceof JsonArray) { //discouraged, since it's leaking store-specifics in the method signature
      query.startKey((JsonArray) next);
    } else if (next instanceof JsonObject) { //discouraged, since it's leaking store-specifics in the method signature
      query.startKey((JsonObject) next);
    } else {
      throw new IllegalArgumentException("Unsupported parameter type for startKey: " + next.getClass());
    }
  }
  private void endKey(Iterator<Object> iterator) {
    if (!iterator.hasNext()) {
      throw new IllegalArgumentException("Not enough parameters for endKey");
    }

    Object next = iterator.next();
    if (next instanceof String) {
      query.endKey((String) next);
    } else if (next instanceof Boolean) {
      query.endKey((Boolean) next);
    } else if (next instanceof Double) {
      query.endKey((Double) next);
    } else if (next instanceof Integer) {
      query.endKey((Integer) next);
    } else if (next instanceof Long) {
      query.endKey((Long) next);
    } else if (next instanceof Collection) {
      //when creating a JsonArray, the from(List) method is preferred because it will convert internal
      //Lists and Maps to JsonObject and JsonArray respectively
      List<Object> arrayContent = new ArrayList<Object>((Collection) next);
      query.endKey(JsonArray.from(arrayContent));
    } else if (next.getClass().isArray()) {
      List<Object> arrayContent = Arrays.asList((Object[]) next);
      query.endKey(JsonArray.from(arrayContent));
    } else if (next instanceof JsonArray) { //discouraged, since it's leaking store-specifics in the method signature
      query.endKey((JsonArray) next);
    } else if (next instanceof JsonObject) { //discouraged, since it's leaking store-specifics in the method signature
      query.endKey((JsonObject) next);
    } else {
      throw new IllegalArgumentException("Unsupported parameter type for endKey: " + next.getClass());
    }
  }

  private void key(Iterator<Object> iterator) {
    if (!iterator.hasNext() && treeCount > 1) {
      throw new IllegalArgumentException("Not enough parameters for key");
    } else if (!iterator.hasNext()) {
      //probably pattern like findAllByUsername(), just apply query without parameters
      return;
    }

    Object next = iterator.next();
    if (next instanceof String) {
      query.key((String) next);
    } else if (next instanceof Boolean) {
      query.key((Boolean) next);
    } else if (next instanceof Double) {
      query.key((Double) next);
    } else if (next instanceof Integer) {
      query.key((Integer) next);
    } else if (next instanceof Long) {
      query.key((Long) next);
    }  else if (next instanceof Collection) {
      //when creating a JsonArray, the from(List) method is preferred because it will convert internal
      //Lists and Maps to JsonObject and JsonArray respectively
      List<Object> arrayContent = new ArrayList<Object>((Collection) next);
      query.key(JsonArray.from(arrayContent));
    } else if (next.getClass().isArray()) {
      List<Object> arrayContent = Arrays.asList((Object[]) next);
      query.key(JsonArray.from(arrayContent));
    } else if (next instanceof JsonArray) { //discouraged, since it's leaking store-specifics in the method signature
      query.key((JsonArray) next);
    } else if (next instanceof JsonObject) { //discouraged, since it's leaking store-specifics in the method signature
      query.key((JsonObject) next);
    } else {
      throw new IllegalArgumentException("Unsupported parameter type for key: " + next.getClass());
    }
  }


  private String nextString(Iterator<Object> iterator) {
    if (!iterator.hasNext()) {
      throw new IllegalStateException("Not enough parameters");
    }

    Object next = iterator.next();
    if (!(next instanceof String)) {
      throw new IllegalArgumentException("Expected String, got " + next.getClass().getName());
    }

    return (String) next;
  }

  private JsonArray in(Iterator<Object> iterator) {
    if (!iterator.hasNext()) {
      throw new IllegalArgumentException("Not enough parameters for in");
    }
    Object next = iterator.next();

    List<Object> values;
    if (next instanceof Collection) {
      values = new ArrayList<Object>((Collection<?>) next);
    } else if (next.getClass().isArray()) {
      values = Arrays.asList((Object[]) next);
    } else {
      values = Collections.singletonList(next);
    }
    //using the from(List) method ensure that contained Lists and Maps will be converted to JsonArrays and JsonObjects
    return JsonArray.from(values);
  }

  @Override
  protected ViewQuery and(Part part, ViewQuery base, Iterator<Object> iterator) {
    return create(part, iterator);//and not really supported, all query derivation mutate the original ViewQuery
  }

  @Override
  protected ViewQuery or(ViewQuery base, ViewQuery criteria) {
    //this won't be called unless there's a Or keyword in the method
    throw new UnsupportedOperationException("Or is not supported for View-based queries");
  }

  @Override
  protected DerivedViewQuery complete(ViewQuery criteria, Sort sort) {
    boolean descending = false;

    if (sort != null) {
      int sortCount = 0;
      Iterator<Sort.Order> it = sort.iterator();
      while(it.hasNext()) {
        sortCount++;
        if (!it.next().isAscending()) {
          descending = true;
        }
      }
      if (sortCount > 1) {
        throw new IllegalArgumentException("Detected " + sortCount + " sort instructions, maximum one supported");
      }
      query.descending(descending);
    }

    if (tree.isLimiting()) {
      query.limit(tree.getMaxResults());
    }

    boolean isCount = tree.isCountProjection() == Boolean.TRUE;
    boolean isExplicitReduce = viewAnnotation != null && viewAnnotation.reduce();
    if (isCount || isExplicitReduce) {
      query.reduce();
    }

    return new DerivedViewQuery(query, tree.isLimiting(), isCount || isExplicitReduce);
  }

  /**
   * Wrapper class allowing to see downstream if the built query was built with options like reduce and limit.
   */
  protected static class DerivedViewQuery {
    public final ViewQuery builtQuery;
    public final boolean isLimited;
    public final boolean isReduce;

    public DerivedViewQuery(ViewQuery builtQuery, boolean isLimited, boolean isReduce) {
      this.builtQuery = builtQuery;
      this.isLimited = isLimited;
      this.isReduce = isReduce;
    }
  }
}
