/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core;

import com.couchbase.client.ObservedException;
import com.couchbase.client.ObservedModifiedException;
import com.couchbase.client.ObservedTimeoutException;
import com.couchbase.client.protocol.views.InvalidViewException;
import com.couchbase.client.vbucket.ConnectionException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import java.util.concurrent.CancellationException;


/**
 * Simple {@link PersistenceExceptionTranslator} for Couchbase.
 * <p/>
 * Convert the given runtime exception to an appropriate exception from the {@code org.springframework.dao} hierarchy.
 * Return {@literal null} if no translation is appropriate: any other exception may have resulted from user code, and
 * should not be translated.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseExceptionTranslator implements PersistenceExceptionTranslator {

  /**
   * Translate Couchbase specific exceptions to spring exceptions if possible.
   *
   * @param ex the exception to translate.
   *
   * @return the translated exception or null.
   */
  @Override
  public final DataAccessException translateExceptionIfPossible(final RuntimeException ex) {

    if (ex instanceof ConnectionException) {
      return new DataAccessResourceFailureException(ex.getMessage(), ex);
    }

    if (ex instanceof ObservedException
            || ex instanceof ObservedTimeoutException
            || ex instanceof ObservedModifiedException) {
      return new DataIntegrityViolationException(ex.getMessage(), ex);
    }

    if (ex instanceof CancellationException) {
      throw new OperationCancellationException(ex.getMessage(), ex);
    }

    if (ex instanceof InvalidViewException) {
      throw new InvalidDataAccessResourceUsageException(ex.getMessage(), ex);
    }

    // Unable to translate exception, therefore just throw the original!
    throw ex;
  }

}
