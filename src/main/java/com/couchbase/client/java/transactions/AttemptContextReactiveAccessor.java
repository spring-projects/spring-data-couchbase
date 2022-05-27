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
import java.util.function.Consumer;
import java.util.logging.Logger;

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
import reactor.util.annotation.Nullable;

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
		return new ReactiveTransactionAttemptContext(getCore(atr), serializer);
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
			return new TransactionAttemptContext(getCore(atr), serializer);
	}

	public static CoreTransactionLogger getLogger(ReactiveTransactionAttemptContext attemptContextReactive) {
		return attemptContextReactive.logger();
	}

	public static CoreTransactionLogger getLogger(TransactionAttemptContext attemptContextReactive) {
		return attemptContextReactive.logger();
	}

	// todo gp needed?
	@Stability.Internal
	public static CoreTransactionAttemptContext newCoreTranactionAttemptContext(ReactiveTransactions transactions) {

		String txnId = UUID.randomUUID().toString();
		CoreTransactionsReactive coreTransactionsReactive;
		try {
			Field field = ReactiveTransactions.class.getDeclaredField("internal");
			field.setAccessible(true);
			coreTransactionsReactive = (CoreTransactionsReactive) field.get(transactions);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}

		CoreTransactionOptions perConfig = new CoreTransactionOptions(Optional.empty(), Optional.empty(), Optional.empty(),
				Optional.of(Duration.ofMinutes(10)), Optional.empty(), Optional.empty());

		CoreMergedTransactionConfig merged = new CoreMergedTransactionConfig(coreTransactionsReactive.config(),
				Optional.ofNullable(perConfig));
		CoreTransactionContext overall = new CoreTransactionContext(
				coreTransactionsReactive.core().context().environment().requestTracer(),
				coreTransactionsReactive.core().context().environment().eventBus(), UUID.randomUUID().toString(), merged,
				coreTransactionsReactive.core().transactionsCleanup());

		CoreTransactionAttemptContext coreTransactionAttemptContext = coreTransactionsReactive.createAttemptContext(overall,
				merged, txnId);
		return coreTransactionAttemptContext;
	}

	private static Duration now() {
		return Duration.of(System.nanoTime(), ChronoUnit.NANOS);
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
		try {
			Field field = TransactionAttemptContext.class.getDeclaredField("internal");
			field.setAccessible(true);
			return (CoreTransactionAttemptContext) field.get(atr);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
	}

	public static Mono<Void> implicitCommit(ReactiveTransactionAttemptContext atr, boolean b) {
		CoreTransactionAttemptContext coreTransactionsReactive = getCore(atr);
		try {
			// getDeclaredMethod() does not find it (because of primitive arg?)
			// CoreTransactionAttemptContext.class.getDeclaredMethod("implicitCommit", Boolean.class);
			Method[] methods = CoreTransactionAttemptContext.class.getDeclaredMethods();
			Method method = null;
			for (Method m : methods) {
				if (m.getName().equals("implicitCommit")) {
					method = m;
					break;
				}
			}
			if (method == null) {
				throw new RuntimeException("did not find implicitCommit method");
			}
			method.setAccessible(true);
			return (Mono<Void>) method.invoke(coreTransactionsReactive, b);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}

	}

	public static AttemptState getState(ReactiveTransactionAttemptContext atr) {
		CoreTransactionAttemptContext coreTransactionsReactive = getCore(atr);
		try {
			Field field = CoreTransactionAttemptContext.class.getDeclaredField("state");
			field.setAccessible(true);
			return (AttemptState) field.get(coreTransactionsReactive);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
	}

	public static ReactiveTransactionAttemptContext createReactiveTransactionAttemptContext(
			CoreTransactionAttemptContext core, JsonSerializer jsonSerializer) {
		return new ReactiveTransactionAttemptContext(core, jsonSerializer);
	}

	public static CoreTransactionsReactive getCoreTransactionsReactive(ReactiveTransactions transactions) {
		try {
			Field field = ReactiveTransactions.class.getDeclaredField("internal");
			field.setAccessible(true);
			return (CoreTransactionsReactive) field.get(transactions);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
	}

	public static TransactionAttemptContext newTransactionAttemptContext(CoreTransactionAttemptContext ctx,
			JsonSerializer jsonSerializer) {
		return new TransactionAttemptContext(ctx, jsonSerializer);
	}

	public static TransactionResult run(Transactions transactions, Consumer<TransactionAttemptContext> transactionLogic, CoreTransactionOptions coreTransactionOptions) {
		return reactive(transactions).runBlocking(transactionLogic, coreTransactionOptions);
	}

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
