/*
 * Copyright 2017-present the original author or authors
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
package org.springframework.data.couchbase.core.support;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.CouchbaseExceptionTranslator;
import org.springframework.data.couchbase.core.OperationInterruptedException;
import org.springframework.data.couchbase.repository.query.CountFragment;

/**
 * @author Subhashni Balakrishnan
 * @author Michael Reiche
 * @since 3.0
 */
public class TemplateUtils {
	public static final String SELECT_ID = "__id";
	public static final String SELECT_CAS = "__cas";
	public static final String SELECT_ID_3x = "_ID";
	public static final String SELECT_CAS_3x = "_CAS";
	public static final String SELECT_COUNT = CountFragment.COUNT_ALIAS;
	private static PersistenceExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();

	public static Throwable translateError(Throwable e) {
		if (e instanceof RuntimeException) {
			return exceptionTranslator.translateExceptionIfPossible((RuntimeException) e);
		} else if (e instanceof TimeoutException) {
			return new QueryTimeoutException(e.getMessage(), e);
		} else if (e instanceof InterruptedException) {
			return new OperationInterruptedException(e.getMessage(), e);
		} else if (e instanceof ExecutionException) {
			return new OperationInterruptedException(e.getMessage(), e);
		} else {
			return e;
		}
	}
}
