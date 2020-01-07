/*
 * Copyright 2012-2020 the original author or authors
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

package org.springframework.data.couchbase.repository.query;

/**
 * An utility entity that allows to extract total row count out of a COUNT(*) N1QL query.
 * <p/>
 * The query should use the COUNT_ALIAS, eg.: SELECT COUNT(*) AS count FROM default;
 * <p/>
 * This ensures that the framework will be able to map the JSON result to this {@link CountFragment} class
 * so that it can be used.
 */
public class CountFragment {

  /**
   * Use this alias for the COUNT part of a N1QL query so that the framework can extract the count result.
   * Eg.: "SELECT A.COUNT(*) AS " + COUNT_ALIAS + " FROM A";
   */
  public static final String COUNT_ALIAS = "count";

  /**
   * The value for a COUNT that used {@link #COUNT_ALIAS} as an alias.
   */
  public long count;

}
