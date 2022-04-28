/*
   * Copyright 2019-2021 the original author or authors.
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

package org.springframework.data.couchbase.transaction;

import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.ResourceHolderSupport;

/**
 * MongoDB specific resource holder, wrapping a {@link ClientSession}. {@link ReactiveCouchbaseTransactionManager} binds
 * instances of this class to the subscriber context.
 * <p />
 * <strong>Note:</strong> Intended for internal usage only.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 * @see CouchbaseTransactionManager
 * @see CouchbaseTemplate
 */
// todo gp understand why this is needed - can we not just hold ctx in Mono context?
public class CouchbaseResourceHolder extends ResourceHolderSupport {

	private @Nullable ClientSession session; // which holds the atr
	private CouchbaseClientFactory databaseFactory;

	/**
	 * Create a new {@link org.springframework.data.couchbase.transaction.ReactiveCouchbaseResourceHolder} for a given
	 * {@link ClientSession session}.
	 *
	 * @param session the associated {@link ClientSession}. Can be {@literal null}.
	 * @param databaseFactory the associated {@link CouchbaseClientFactory}. must not be {@literal null}.
	 */
	public CouchbaseResourceHolder(@Nullable ClientSession session, CouchbaseClientFactory databaseFactory) {

		this.session = session;
		this.databaseFactory = databaseFactory;
	}

	/**
	 * @return the associated {@link ClientSession}. Can be {@literal null}.
	 */
	@Nullable
  public ClientSession getSession() {
		return session;
	}

	/**
	 * @return the required associated {@link ClientSession}.
	 * @throws IllegalStateException if no session is associated.
	 */
	ClientSession getRequiredSession() {

		ClientSession session = getSession();

		if (session == null) {
			throw new IllegalStateException("No ClientSession associated");
		}
		return session;
	}

	/**
	 * @return the associated {@link CouchbaseClientFactory}.
	 */
	public CouchbaseClientFactory getDatabaseFactory() {
		return databaseFactory;
	}

	/**
	 * Set the {@link ClientSession} to guard.
	 *
	 * @param session can be {@literal null}.
	 */
	public void setSession(@Nullable ClientSession session) {
		this.session = session;
	}

	/**
	 * @return {@literal true} if session is not {@literal null}.
	 */
	boolean hasSession() {
		return session != null;
	}

	/**
	 * If the {@link org.springframework.data.couchbase.transaction.ReactiveCouchbaseResourceHolder} is
	 * {@link #hasSession() not already associated} with a {@link ClientSession} the given value is
	 * {@link #setSession(ClientSession) set} and returned, otherwise the current bound session is returned.
	 *
	 * @param session
	 * @return
	 */
	@Nullable
	public ClientSession setSessionIfAbsent(@Nullable ClientSession session) {

		if (!hasSession()) {
			setSession(session);
		}

		return session;
	}

	/**
	 * @return {@literal true} if the session is active and has not been closed.
	 */
	boolean hasActiveSession() {

		if (!hasSession()) {
			return false;
		}

		return hasServerSession() && !getRequiredSession().getServerSession().isClosed();
	}

	/**
	 * @return {@literal true} if the session has an active transaction.
	 * @see #hasActiveSession()
	 */
	boolean hasActiveTransaction() {

		if (!hasActiveSession()) {
			return false;
		}

		return getRequiredSession().hasActiveTransaction();
	}

	/**
	 * @return {@literal true} if the {@link ClientSession} has a {link com.mongodb.session.ServerSession} associated that
	 *         is accessible via {@link ClientSession#getServerSession()}.
	 */
	boolean hasServerSession() {

		try {
			return getRequiredSession().getServerSession() != null;
		} catch (IllegalStateException serverSessionClosed) {
			// ignore
		}

		return false;
	}
}
