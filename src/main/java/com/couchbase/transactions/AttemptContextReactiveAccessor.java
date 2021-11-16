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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.transactions.config.MergedTransactionConfig;
import com.couchbase.transactions.config.PerTransactionConfig;
import com.couchbase.transactions.config.PerTransactionConfigBuilder;
import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.forwards.Supported;
import com.couchbase.transactions.log.TransactionLogger;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

/**
 * To access the AttemptContextReactive held by AttemptContext
 *
 * @author Michael Reiche
 */
public class AttemptContextReactiveAccessor {

  public static AttemptContextReactive getACR(AttemptContext attemptContext) {
    return attemptContext.ctx();
  }

  public static AttemptContext from(AttemptContextReactive attemptContextReactive) {
    return new AttemptContext(attemptContextReactive);
  }

  public static TransactionLogger getLogger(AttemptContextReactive attemptContextReactive){
    return attemptContextReactive.LOGGER;
  }
  @Stability.Internal
  public static AttemptContextReactive newAttemptContextReactive(TransactionsReactive transactions){
    PerTransactionConfig perConfig = PerTransactionConfigBuilder.create().build();
    MergedTransactionConfig merged = new MergedTransactionConfig(transactions.config(), Optional.of(perConfig));

    TransactionContext overall = new TransactionContext(
        transactions.cleanup().clusterData().cluster().environment().requestTracer(),
        transactions.cleanup().clusterData().cluster().environment().eventBus(),
        UUID.randomUUID().toString(), now(), Duration.ZERO, merged);

    String txnId = UUID.randomUUID().toString();
    overall.LOGGER.info(configDebug(transactions.config(), perConfig));
    return transactions.createAttemptContext(overall, merged, txnId);
  }

  private static Duration now() {
    return Duration.of(System.nanoTime(), ChronoUnit.NANOS);
  }

  static private String configDebug(TransactionConfig config, PerTransactionConfig perConfig) {
    StringBuilder sb = new StringBuilder();
    sb.append("library version: ");
    sb.append(TransactionsReactive.class.getPackage().getImplementationVersion());
    sb.append(" config: ");
    sb.append("atrs=");
    sb.append(config.numAtrs());
    sb.append(", metadataCollection=");
    sb.append(config.metadataCollection());
    sb.append(", expiry=");
    sb.append(perConfig.expirationTime().orElse(config.transactionExpirationTime()).toMillis());
    sb.append("msecs durability=");
    sb.append(config.durabilityLevel());
    sb.append(" per-txn config=");
    sb.append(" durability=");
    sb.append(perConfig.durabilityLevel());
    sb.append(", supported=");
    sb.append(Supported.SUPPORTED);
    return sb.toString();
  }

}
