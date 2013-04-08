/**
 * Copyright (C) 2009-2012 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.spring.core;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import com.couchbase.client.ObservedException;
import com.couchbase.client.ObservedModifiedException;
import com.couchbase.client.ObservedTimeoutException;
import com.couchbase.client.vbucket.ConnectionException;

import java.io.IOException;

/**
 * Simple {@link PersistenceExceptionTranslator} for Couchbase.
 * 
 * Convert the given runtime exception to an appropriate exception from the 
 * {@code org.springframework.dao} hierarchy. Return {@literal null} if no translation 
 * is appropriate: any other exception may have resulted from user code, and should not 
 * be translated.
 */
public class CouchbaseExceptionTranslator implements PersistenceExceptionTranslator {

  /**
   * Translate Couchbase specific exceptions to spring exceptions if possible.
   *
   * @param ex the exception to translate.
   * @return the translated exception or null.
   */
	@Override
	public final DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		if (ex instanceof ConnectionException) {
		  return new DataAccessResourceFailureException(ex.getMessage(), ex);
		}
		
		if (ex instanceof ObservedException
      || ex instanceof ObservedTimeoutException
      || ex instanceof ObservedModifiedException) {
			return new DataIntegrityViolationException(ex.getMessage(), ex);
		}
		
		return null;
	}

}
