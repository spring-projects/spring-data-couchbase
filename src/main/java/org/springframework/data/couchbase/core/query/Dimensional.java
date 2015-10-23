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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.QueryAnnotation;

/**
 * An annotation to mark a repository method as querying using a Couchbase Spatial View.
 * The attributes will be used to resolve the actual Spatial View to be used and to determine
 * the expected number of dimensions in the Spatial View keys. It can also be used as meta-annotation.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@QueryAnnotation
public @interface Dimensional {
  /** The name of the design document to be queried */
  String designDocument();
  /** The name of the spatial view to be queried */
  String spatialViewName();
  /** The number of dimensions in the spatial view, defaults to 2 */
  int dimensions() default 2;
}
