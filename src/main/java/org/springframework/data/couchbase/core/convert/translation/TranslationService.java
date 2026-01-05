/*
 * Copyright 2012-present the original author or authors
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

package org.springframework.data.couchbase.core.convert.translation;

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseStorable;

/**
 * Defines a translation service to encode/decode responses into the {@link CouchbaseStorable} format.
 *
 * @author Michael Nitschinger
 */
public interface TranslationService {

	/**
	 * Encodes a JSON String into the target format.
	 *
	 * @param source the source contents to encode.
	 * @return the encoded document representation.
	 */
	String encode(CouchbaseStorable source);

	/**
	 * Decodes the target format into a {@link CouchbaseDocument}
	 *
	 * @param source the source formatted document.
	 * @param target the target of the populated data.
	 * @return a properly populated document to work with.
	 */
	CouchbaseStorable decode(String source, CouchbaseStorable target);

	/**
	 * Decodes an ad-hoc JSON object into a corresponding "case" class.
	 *
	 * @param source the JSON for the ad-hoc JSON object (from a N1QL query for instance).
	 * @param target the target class information.
	 * @param <T> the target class.
	 * @return an ad-hoc instance of the decoded JSON into the corresponding "case" class.
	 */
	<T> T decodeFragment(String source, Class<T> target);
}
