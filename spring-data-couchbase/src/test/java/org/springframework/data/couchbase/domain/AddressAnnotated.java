/*
 * Copyright 2020-2022 the original author or authors
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

package org.springframework.data.couchbase.domain;

import org.springframework.data.couchbase.repository.Collection;
import org.springframework.data.couchbase.repository.Scope;

/**
 * @author Michael Reiche
 */
@Scope("dummy_scope") // set to non-existing scope. To use, scope must be determined by other means
// a different collection
@Collection("my_collection2")
public class AddressAnnotated extends Address {}
