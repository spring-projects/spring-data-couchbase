/*
 * Copyright 2017 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Value object to capture custom conversion.
 * <p/>
 * <p>Types that can be mapped directly onto JSON are considered simple ones, because they neither need deeper
 * inspection nor nested conversion.</p>
 *
 * @author Michael Nitschinger
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Subhashni Balakrishnan
 * @see org.springframework.data.convert.CustomConversions
 * @see SimpleTypeHolder
 * @since 2.0
 */
public class CouchbaseCustomConversions extends org.springframework.data.convert.CustomConversions {

  private static final StoreConversions STORE_CONVERSIONS;

  private static final List<Object> STORE_CONVERTERS;

  static {

    List<Object> converters = new ArrayList<>();

    converters.addAll(DateConverters.getConvertersToRegister());
    converters.addAll(CouchbaseJsr310Converters.getConvertersToRegister());

    STORE_CONVERTERS = Collections.unmodifiableList(converters);
    STORE_CONVERSIONS = StoreConversions.of(SimpleTypeHolder.DEFAULT, STORE_CONVERTERS);
  }

  /**
   * Create a new instance with a given list of converters.
   *
   * @param converters the list of custom converters.
   */
  public CouchbaseCustomConversions(final List<?> converters) {
    super(STORE_CONVERSIONS, converters);
  }
}
