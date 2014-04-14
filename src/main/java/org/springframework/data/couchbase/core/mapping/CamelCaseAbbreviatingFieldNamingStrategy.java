/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.couchbase.core.mapping;

/**
 * {@link FieldNamingStrategy} that abbreviates field names by using the very first letter of the camel case parts of
 * the {@link CouchbasePersistentProperty}'s name.
 *
 * @since 1.1
 * @author Michael Nitschinger
 * @author Oliver Gierke
 */
public class CamelCaseAbbreviatingFieldNamingStrategy extends CamelCaseSplittingFieldNamingStrategy {

  public CamelCaseAbbreviatingFieldNamingStrategy() {
    super("");
  }

  @Override
  protected String preparePart(String part) {
    return part.substring(0, 1);
  }


}
