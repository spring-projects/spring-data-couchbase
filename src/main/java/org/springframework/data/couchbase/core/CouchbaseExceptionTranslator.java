/*
 * Copyright 2012-2025 the original author or authors
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

import com.couchbase.client.core.error.subdoc.*;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.transaction.error.UncategorizedTransactionDataAccessException;

import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.DesignDocumentNotFoundException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentLockedException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DurabilityAmbiguousException;
import com.couchbase.client.core.error.DurabilityImpossibleException;
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.error.ReplicaNotConfiguredException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.core.error.ServiceNotAvailableException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.error.ValueTooLargeException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;

/**
 * Simple {@link PersistenceExceptionTranslator} for Couchbase.
 * <p>
 * Convert the given runtime exception to an appropriate exception from the {@code org.springframework.dao} hierarchy.
 * Return {@literal null} if no translation is appropriate: any other exception may have resulted from user code, and
 * should not be translated.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Michael Reiche
 * @author Graham Pople
 * @author Tigran Babloyan
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

		if (ex instanceof ReplicaNotConfiguredException || ex instanceof DurabilityLevelNotAvailableException
				|| ex instanceof DurabilityImpossibleException || ex instanceof DurabilityAmbiguousException) {
			return new DataIntegrityViolationException(ex.getMessage(), ex);
		}

		if (ex instanceof RequestCanceledException) {
			return new OperationCancellationException(ex.getMessage(), ex);
		}

		if (ex instanceof DesignDocumentNotFoundException || ex instanceof ValueTooLargeException
				|| ex instanceof PathExistsException || ex instanceof PathInvalidException
				|| ex instanceof PathNotFoundException || ex instanceof PathMismatchException
				|| ex instanceof PathTooDeepException || ex instanceof ValueInvalidException
				|| ex instanceof ValueTooDeepException || ex instanceof DocumentTooDeepException) {
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

		if (ex instanceof TransactionOperationFailedException transactionOperationFailedException) {
			// Replace the TransactionOperationFailedException, since we want the Spring operation to fail with a
			// Spring error. Internal state has already been set in the AttemptContext so the retry, rollback etc.
			// will get respected regardless of what gets propagated (or not) from the lambda.
			return new UncategorizedTransactionDataAccessException(transactionOperationFailedException);
		}

		// Unable to translate exception, therefore just throw the original!
		throw ex;
	}

}
