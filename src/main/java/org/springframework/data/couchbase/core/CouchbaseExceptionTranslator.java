/*
 * Copyright 2012-2023 the original author or authors
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

package org.springframework.data.couchbase.core;

import java.util.ConcurrentModificationException;
import java.util.concurrent.TimeoutException;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.OptimisticLockingFailureException;;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import com.couchbase.client.core.error.*;

/**
 * Simple {@link PersistenceExceptionTranslator} for Couchbase.
 * <p>
 * Convert the given runtime exception to an appropriate exception from the {@code org.springframework.dao} hierarchy.
 * Return {@literal null} if no translation is appropriate: any other exception may have resulted from user code, and
 * should not be translated.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 */
public class CouchbaseExceptionTranslator implements PersistenceExceptionTranslator {

	/**
	 * Translate Couchbase specific exceptions to spring exceptions if possible.
	 *
	 * @param ex the exception to translate.
	 * @return the translated exception or null.
	 */
	@Override
	public final DataAccessException translateExceptionIfPossible(final RuntimeException ex) {

		if (ex instanceof ConfigException || ex instanceof ServiceNotAvailableException
				|| ex instanceof CollectionNotFoundException || ex instanceof ScopeNotFoundException
				|| ex instanceof BucketNotFoundException) {
			return new DataAccessResourceFailureException(ex.getMessage(), ex);
		}

		if (ex instanceof DocumentExistsException) {
			return new DuplicateKeyException(ex.getMessage(), ex);
		}

		if (ex instanceof DocumentNotFoundException) {
			return new DataRetrievalFailureException(ex.getMessage(), ex);
		}

		if (ex instanceof CasMismatchException || ex instanceof ConcurrentModificationException) {
			return new OptimisticLockingFailureException(ex.getMessage(), ex);
		}

		if ( ex instanceof ReplicaNotConfiguredException || ex instanceof DurabilityLevelNotAvailableException
				|| ex instanceof DurabilityImpossibleException || ex instanceof DurabilityAmbiguousException) {
			return new DataIntegrityViolationException(ex.getMessage(), ex);
		}

		if (ex instanceof RequestCanceledException) {
			return new OperationCancellationException(ex.getMessage(), ex);
		}

		if (ex instanceof DesignDocumentNotFoundException || ex instanceof ValueTooLargeException) {
			return new InvalidDataAccessResourceUsageException(ex.getMessage(), ex);
		}

		if (ex instanceof TemporaryFailureException || ex instanceof DocumentLockedException) {
			return new TransientDataAccessResourceException(ex.getMessage(), ex);
		}

		if ((ex instanceof RuntimeException && ex.getCause() instanceof TimeoutException)) {
			return new QueryTimeoutException(ex.getMessage(), ex);
		}

		if (ex instanceof EncodingFailureException || ex instanceof DecodingFailureException) {
			// note: the more specific CouchbaseQueryExecutionException should be thrown by the template
			// when dealing with TranscodingException in the query/n1ql methods.
			return new DataRetrievalFailureException(ex.getMessage(), ex);
		}

		// Unable to translate exception, therefore just throw the original!
		throw ex;
	}

}
