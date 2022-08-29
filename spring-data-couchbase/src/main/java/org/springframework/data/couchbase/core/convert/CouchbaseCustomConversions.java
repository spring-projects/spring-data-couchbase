/*
 * Copyright 2017-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.convert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.mapping.model.SimpleTypeHolder;

import com.couchbase.client.core.encryption.CryptoManager;

/**
 * Value object to capture custom conversion.
 * <p>
 * Types that can be mapped directly onto JSON are considered simple ones, because they neither need deeper inspection
 * nor nested conversion.
 *
 * @author Michael Nitschinger
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Subhashni Balakrishnan
 * @Michael Reiche
 * @see org.springframework.data.convert.CustomConversions
 * @see SimpleTypeHolder
 * @since 2.0
 */
public class CouchbaseCustomConversions extends org.springframework.data.convert.CustomConversions {

	private static final StoreConversions STORE_CONVERSIONS;

	private static final List<Object> STORE_CONVERTERS;

	private CryptoManager cryptoManager;

	/**
	 * Expose the CryptoManager used by a DecryptingReadingConverter or EncryptingWritingConverter, if any. There can only
	 * be one. MappingCouchbaseConverter needs it.
	 * 
	 * @return cryptoManager
	 */
	public CryptoManager getCryptoManager() {
		return cryptoManager;
	}

	static {

		List<Object> converters = new ArrayList<>();

		converters.addAll(DateConverters.getConvertersToRegister());
		converters.addAll(CouchbaseJsr310Converters.getConvertersToRegister());
		converters.addAll(OtherConverters.getConvertersToRegister());

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
		for (Object c : converters) {
			if (c instanceof DecryptingReadingConverter) {
				CryptoManager foundCryptoManager = ((DecryptingReadingConverter) c).cryptoManager;
				if (foundCryptoManager == null) {
					throw new RuntimeException(("DecryptingReadingConverter must have a cryptoManager"));
				} else {
					if (cryptoManager != null && this.cryptoManager != cryptoManager) {
						throw new RuntimeException(
								"all DecryptingReadingConverters and EncryptingWringConverters must use " + " a single CryptoManager");
					}
				}
				cryptoManager = foundCryptoManager;
			}
			if (c instanceof EncryptingWritingConverter) {
				CryptoManager foundCryptoManager = ((EncryptingWritingConverter) c).cryptoManager;
				if (foundCryptoManager == null) {
					throw new RuntimeException(("EncryptingWritingConverter must have a cryptoManager"));
				} else {
					if (cryptoManager != null && this.cryptoManager != cryptoManager) {
						throw new RuntimeException(
								"all DecryptingReadingConverters and EncryptingWringConverters must use " + " a single CryptoManager");
					}
				}
				cryptoManager = foundCryptoManager;
			}
		}
	}
}
