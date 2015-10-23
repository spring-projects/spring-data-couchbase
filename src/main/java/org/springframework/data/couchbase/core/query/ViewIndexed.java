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

package org.springframework.data.couchbase.core.query;
 import org.springframework.data.couchbase.repository.CouchbaseRepository;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is targeted at {@link CouchbaseRepository Repository} interfaces, indicating that
 * the framework should ensure a View is present when the repository is instantiated.
 * <p/>
 * The view must at least be described as a design document name and view name. Default map function
 * will filter documents on the type associated to the repository, and default reduce function is "_count".
 * <p/>
 * One can specify a custom reduce function as well as a non-default map function. This can be done on methods,
 * allowing for multiple views per repository to be created.
 *
 * @author Simon Baslé
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ViewIndexed {

  /**
   * The design document in which to create/look for the view.
   */
  String designDoc();

  /**
   * The name of the view.
   */
  String viewName();

  /**
   * The map function to use (default is empty, which will trigger a default map function filtering on the
   * repository's associated entity type).
   */
  String mapFunction() default "";

  /**
   * The reduce function to use (default is built in "_count" reduce function).
   */
  String reduceFunction() default "_count";
}
