/*
 * Copyright 2012-2017 the original author or authors
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

package org.springframework.data.couchbase.core.convert;

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
 * @deprecated since 2.0, use {@link CouchbaseCustomConversions}.
 */
@Deprecated
public class CustomConversions extends CouchbaseCustomConversions {

  /**
   * Create a new instance with a given list of conversers.
   *
   * @param converters the list of custom converters.
   */
  public CustomConversions(final List<?> converters) {
    super(converters);
  }
}
