/*
 * Copyright 2017 the original author or authors
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

package org.springframework.data.couchbase.core.mapping.id;
import static org.springframework.data.couchbase.core.mapping.id.GenerationStrategy.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.data.couchbase.core.mapping.Document;

/**
 * This annotation is targeted at {@link Document} interfaces, indicating that
 * the framework should generate the document key value using the specified generator
 *
 * @author Subhashni Balakrishnan
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface GeneratedValue {

	String delimiter() default ".";

	GenerationStrategy strategy() default USE_ATTRIBUTES;
}