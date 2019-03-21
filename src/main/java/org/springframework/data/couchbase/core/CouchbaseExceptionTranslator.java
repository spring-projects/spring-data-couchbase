/*
 * Copyright 2012-2015 the original author or authors
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

import java.util.concurrent.TimeoutException;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.BucketClosedException;
import com.couchbase.client.core.DocumentConcurrentlyModifiedException;
import com.couchbase.client.core.ReplicaNotConfiguredException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.ServiceNotAvailableException;
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.core.endpoint.SSLException;
import com.couchbase.client.core.endpoint.kv.AuthenticationException;
import com.couchbase.client.core.env.EnvironmentException;
import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.java.error.BucketDoesNotExistException;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DesignDocumentException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.DurabilityException;
import com.couchbase.client.java.error.InvalidPasswordException;
import com.couchbase.client.java.error.RequestTooBigException;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.error.TemporaryLockFailureException;
import com.couchbase.client.java.error.TranscodingException;
import com.couchbase.client.java.error.ViewDoesNotExistException;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.dao.support.PersistenceExceptionTranslator;


/**
 * Simple {@link PersistenceExceptionTranslator} for Couchbase.
 * <p/>
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

    if (ex instanceof InvalidPasswordException
        || ex instanceof NotConnectedException
        || ex instanceof ConfigurationException
        || ex instanceof EnvironmentException
        || ex instanceof InvalidPasswordException
        || ex instanceof SSLException
        || ex instanceof ServiceNotAvailableException
        || ex instanceof BucketClosedException
        || ex instanceof BucketDoesNotExistException
        || ex instanceof AuthenticationException) {
      return new DataAccessResourceFailureException(ex.getMessage(), ex);
    }

    if (ex instanceof DocumentAlreadyExistsException) {
      return new DuplicateKeyException(ex.getMessage(), ex);
    }

    if (ex instanceof DocumentDoesNotExistException) {
      return new DataRetrievalFailureException(ex.getMessage(), ex);
    }

    if (ex instanceof CASMismatchException
        || ex instanceof DocumentConcurrentlyModifiedException
        || ex instanceof ReplicaNotConfiguredException
        || ex instanceof DurabilityException) {
      return new DataIntegrityViolationException(ex.getMessage(), ex);
    }

    if (ex instanceof RequestCancelledException
        || ex instanceof BackpressureException) {
      return new OperationCancellationException(ex.getMessage(), ex);
    }

    if (ex instanceof ViewDoesNotExistException
        || ex instanceof RequestTooBigException
        || ex instanceof DesignDocumentException) {
      return new InvalidDataAccessResourceUsageException(ex.getMessage(), ex);
    }

    if (ex instanceof TemporaryLockFailureException
        || ex instanceof TemporaryFailureException) {
      return new TransientDataAccessResourceException(ex.getMessage(), ex);
    }

    if ((ex instanceof RuntimeException && ex.getCause() instanceof TimeoutException)) {
      return new QueryTimeoutException(ex.getMessage(), ex);
    }

    if (ex instanceof TranscodingException) {
      //note: the more specific CouchbaseQueryExecutionException should be thrown by the template
      //when dealing with TranscodingException in the query/n1ql methods.
      return new DataRetrievalFailureException(ex.getMessage(), ex);
    }

    // Unable to translate exception, therefore just throw the original!
    throw ex;
  }

}
