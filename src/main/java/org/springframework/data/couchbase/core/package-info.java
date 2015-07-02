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

/**
 * This package contains the specific implementations and core classes for
 * Spring Data Couchbase internals. It also contains Couchbase implementation
 * to support the Spring Data template abstraction.
 * <br/>
 * The template provides lower level access to the underlying database and also serves as the foundation for
 * repositories. Any time a repository is too high-level for you needs chances are good that the templates will serve
 * you well.
 */
package org.springframework.data.couchbase.core;