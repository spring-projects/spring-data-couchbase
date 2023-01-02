/*
 * Copyright 2022-2023 the original author or authors
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

import com.couchbase.client.core.transaction.threadlocal.TransactionMarker;
import reactor.core.publisher.Mono;

import java.util.Optional;

import org.springframework.data.couchbase.transaction.CouchbaseResourceHolder;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.threadlocal.TransactionMarkerOwner;
import reactor.util.context.ContextView;

/**
 * Utility methods to support transactions.
 *
 * @author Graham Pople
 */
@Stability.Internal
public class TransactionalSupport {

	/**
	 * Returns non-empty iff in a transaction. It determines this from thread-local storage and/or reactive context.
	 * <p>
	 * The user could be doing a reactive operation (with .block()) inside a blocking transaction (like @Transactional).
	 * Or a blocking operation inside a ReactiveTransactionsWrapper transaction (which would be a bad idea). So, need to
	 * check both thread-local storage and reactive context.
	 */
	public static Mono<Optional<CouchbaseResourceHolder>> checkForTransactionInThreadLocalStorage() {
		return TransactionMarkerOwner.get().flatMap(markerOpt -> {
			Optional<CouchbaseResourceHolder> out = markerOpt
					.flatMap(marker -> Optional.of(new CouchbaseResourceHolder(marker.context())));
			return Mono.just(out);
		});
	}

	public static Mono<Void> verifyNotInTransaction(String methodName) {
		return checkForTransactionInThreadLocalStorage().flatMap(s -> {
			if (s.isPresent()) {
				return Mono.error(new IllegalArgumentException(methodName + " can not be used inside a transaction"));
			} else {
				return Mono.empty();
			}
		});
	}

	public static RuntimeException retryTransactionOnCasMismatch(CoreTransactionAttemptContext ctx, long cas1,
			long cas2) {
		try {
			ctx.logger().info(ctx.attemptId(), "Spring CAS mismatch %s != %s, retrying transaction", cas1, cas2);
			TransactionOperationFailedException err = TransactionOperationFailedException.Builder.createError()
					.retryTransaction().cause(new CasMismatchException(null)).build();
			return ctx.operationFailed(err);
		} catch (Throwable err) {
			return new RuntimeException(err);
		}

	}
}
