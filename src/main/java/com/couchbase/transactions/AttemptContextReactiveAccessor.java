/*
/*
 * Copyright 2021 the original author or authors
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
package com.couchbase.transactions;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionAttemptContext;

/**
 * To access the ReactiveTransactionAttemptContext held by TransactionAttemptContext
 *
 * @author Michael Reiche
 */
public class AttemptContextReactiveAccessor {

	public static ReactiveTransactionAttemptContext getACR(TransactionAttemptContext attemptContext) {
		// return attemptContext.ctx();
		// todo gp is this access needed. Could hold the raw CoreTransactionAttemptContext instead.
		return null;
	}

	public static TransactionAttemptContext from(ReactiveTransactionAttemptContext attemptContextReactive) {
		// todo gp needed?
		return null;
		// return new TransactionAttemptContext(attemptContextReactive);
	}

	public static CoreTransactionLogger getLogger(ReactiveTransactionAttemptContext attemptContextReactive) {
		// todo gp needed?
		return null;
		// return attemptContextReactive;
	}
	// todo gp needed?
	// @Stability.Internal
	// public static ReactiveTransactionAttemptContext newAttemptContextReactive(TransactionsReactive transactions){
	// return null;
	// PerTransactionConfig perConfig = PerTransactionConfigBuilder.create().build();
	// MergedTransactionConfig merged = new MergedTransactionConfig(transactions.config(), Optional.of(perConfig));
	//
	// TransactionContext overall = new TransactionContext(
	// transactions.cleanup().clusterData().cluster().environment().requestTracer(),
	// transactions.cleanup().clusterData().cluster().environment().eventBus(),
	// UUID.randomUUID().toString(), now(), Duration.ZERO, merged);
	//
	// String txnId = UUID.randomUUID().toString();
	// overall.LOGGER.info(configDebug(transactions.config(), perConfig));
	// return transactions.createAttemptContext(overall, merged, txnId);
	// }

	private static Duration now() {
		return Duration.of(System.nanoTime(), ChronoUnit.NANOS);
	}

	// todo gp if needed let's expose in the SDK
	// static private String configDebug(TransactionConfig config, PerTransactionConfig perConfig) {
	// StringBuilder sb = new StringBuilder();
	// sb.append("library version: ");
	// sb.append(TransactionsReactive.class.getPackage().getImplementationVersion());
	// sb.append(" config: ");
	// sb.append("atrs=");
	// sb.append(config.numAtrs());
	// sb.append(", metadataCollection=");
	// sb.append(config.metadataCollection());
	// sb.append(", expiry=");
	// sb.append(perConfig.expirationTime().orElse(config.transactionExpirationTime()).toMillis());
	// sb.append("msecs durability=");
	// sb.append(config.durabilityLevel());
	// sb.append(" per-txn config=");
	// sb.append(" durability=");
	// sb.append(perConfig.durabilityLevel());
	// sb.append(", supported=");
	// sb.append(Supported.SUPPORTED);
	// return sb.toString();
	// }

}
