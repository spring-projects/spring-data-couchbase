/*
 * Copyright 2013 the original author or authors.
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

package org.springframework.data.couchbase.core.convert;

import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * Value object to capture custom conversion.
 *
 * <p>Types that can be mapped directly onto JSON are considered simple ones, because they neither need deeper
 * inspection nor nested conversion.</p>
 *
 * @author Michael Nitschinger
 */
public class CustomConversions {

  /**
   * Contains the simple type holder.
   */
  private final SimpleTypeHolder simpleTypeHolder;

  /**
   * Create a new instance with no converters.
   */
  CustomConversions() {
    this(new ArrayList<Object>());
  }

  /**
   * Create a new instance with a given list of conversers.
   * @param converters the list of custom converters.
   */
  public CustomConversions(final List<?> converters) {
    Assert.notNull(converters);

    simpleTypeHolder = new SimpleTypeHolder();
  }

  /**
   * Check that the given type is of "simple type".
   *
   * @param type the type to check.
   * @return if its simple type or not.
   */
  public boolean isSimpleType(final Class<?> type) {
    return simpleTypeHolder.isSimpleType(type);
  }

}
