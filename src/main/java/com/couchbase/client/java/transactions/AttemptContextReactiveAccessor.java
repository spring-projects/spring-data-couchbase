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
package com.couchbase.client.java.transactions;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.CoreTransactionContext;
import com.couchbase.client.core.transaction.CoreTransactionsReactive;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.config.CoreTransactionOptions;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.AttemptState;
import com.couchbase.client.java.codec.JsonSerializer;
import reactor.core.publisher.Mono;

/**
 * To access the ReactiveTransactionAttemptContext held by TransactionAttemptContext
 *
 * @author Michael Reiche
 */
public class AttemptContextReactiveAccessor {

	public static ReactiveTransactions reactive(Transactions transactions) {
		try {
			Field field = Transactions.class.getDeclaredField("reactive");
			field.setAccessible(true);
			return (ReactiveTransactions) field.get(transactions);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
	}

	public static ReactiveTransactionAttemptContext reactive(TransactionAttemptContext atr) {
		JsonSerializer serializer;
		try {
			Field field = TransactionAttemptContext.class.getDeclaredField("serializer");
			field.setAccessible(true);
			serializer = (JsonSerializer) field.get(atr);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
		try {
			Field field = TransactionAttemptContext.class.getDeclaredField("internal");
			field.setAccessible(true);
			return new ReactiveTransactionAttemptContext((CoreTransactionAttemptContext) field.get(atr), serializer);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
	}

	public static TransactionAttemptContext blocking(ReactiveTransactionAttemptContext atr) {
		JsonSerializer serializer;
		try {
			Field field = ReactiveTransactionAttemptContext.class.getDeclaredField("serializer");
			field.setAccessible(true);
			serializer = (JsonSerializer) field.get(atr);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
		try {
			Field field = ReactiveTransactionAttemptContext.class.getDeclaredField("internal");
			field.setAccessible(true);
			return new TransactionAttemptContext((CoreTransactionAttemptContext) field.get(atr), serializer);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
	}

	public static CoreTransactionLogger getLogger(ReactiveTransactionAttemptContext attemptContextReactive) {
		// todo gp it should be possible to get access to CoreTransactionAttemptContext everywhere now, which provides access to the logger
		return null;
		// return attemptContextReactive;
	}

	// todo gpx I'd like to keep these things internal to the transactions logic - we can expose stuff there if needed
	@Stability.Internal
	public static CoreTransactionAttemptContext newCoreTranactionAttemptContext(ReactiveTransactions transactions) {
		// PerTransactionConfig perConfig = PerTransactionConfigBuilder.create().build();
		// MergedTransactionConfig merged = new MergedTransactionConfig(transactions.config(), Optional.of(perConfig));
		//
		// TransactionContext overall = new TransactionContext(
		// transactions.cleanup().clusterData().cluster().environment().requestTracer(),
		// transactions.cleanup().clusterData().cluster().environment().eventBus(),
		// UUID.randomUUID().toString(), now(), Duration.ZERO, merged);

		String txnId = UUID.randomUUID().toString();
		// overall.LOGGER.info(configDebug(transactions.config(), perConfig));

		CoreTransactionsReactive coreTransactionsReactive;
		try {
			Field field = ReactiveTransactions.class.getDeclaredField("internal");
			field.setAccessible(true);
			coreTransactionsReactive = (CoreTransactionsReactive) field.get(transactions);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}

		CoreTransactionOptions perConfig = new CoreTransactionOptions(Optional.empty(),
				Optional.empty(),
				Optional.empty(),
				Optional.of(Duration.ofMinutes(10)),
				Optional.empty(),
				Optional.empty());

		CoreMergedTransactionConfig merged = new CoreMergedTransactionConfig(coreTransactionsReactive.config(),
				Optional.ofNullable(perConfig));
		CoreTransactionContext overall = new CoreTransactionContext(
				coreTransactionsReactive.core().context().environment().requestTracer(),
				coreTransactionsReactive.core().context().environment().eventBus(), UUID.randomUUID().toString(), merged,
				coreTransactionsReactive.core().transactionsCleanup());
		// overall.LOGGER.info(configDebug(config, perConfig, cleanup.clusterData().cluster().core()));

		CoreTransactionAttemptContext coreTransactionAttemptContext = coreTransactionsReactive.createAttemptContext(overall,
				merged, txnId);
		return coreTransactionAttemptContext;
		// ReactiveTransactionAttemptContext reactiveTransactionAttemptContext = new ReactiveTransactionAttemptContext(
		// coreTransactionAttemptContext, null);
		// return reactiveTransactionAttemptContext;
	}

	public static ReactiveTransactionAttemptContext from(CoreTransactionAttemptContext coreTransactionAttemptContext,
			JsonSerializer serializer) {
		TransactionAttemptContext tac = new TransactionAttemptContext(coreTransactionAttemptContext, serializer);
		return reactive(tac);
	}

	public static CoreTransactionAttemptContext getCore(ReactiveTransactionAttemptContext atr) {
		CoreTransactionAttemptContext coreTransactionsReactive;
		try {
			Field field = ReactiveTransactionAttemptContext.class.getDeclaredField("internal");
			field.setAccessible(true);
			coreTransactionsReactive = (CoreTransactionAttemptContext) field.get(atr);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
		return coreTransactionsReactive;
	}

	public static CoreTransactionAttemptContext getCore(TransactionAttemptContext atr) {
		CoreTransactionAttemptContext coreTransactionsReactive;
		try {
			Field field = TransactionAttemptContext.class.getDeclaredField("internal");
			field.setAccessible(true);
			coreTransactionsReactive = (CoreTransactionAttemptContext) field.get(atr);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
		return coreTransactionsReactive;
	}

	public static ReactiveTransactionAttemptContext createReactiveTransactionAttemptContext(CoreTransactionAttemptContext core, JsonSerializer jsonSerializer) {
		return new ReactiveTransactionAttemptContext(core, jsonSerializer);
	}
}
