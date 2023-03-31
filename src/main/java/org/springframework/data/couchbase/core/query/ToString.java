/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.couchbase.core.query;

import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.UpsertOptions;

/**
 * Methods to build string from different types of options
 * @author Shubham Mishra (only refactoring from OptionsBuilder.java)
 */
public class ToString{
	public static String toString(InsertOptions o) {
		StringBuilder s = new StringBuilder();
		InsertOptions.Built b = o.build();
		s.append("{");
		s.append("durabilityLevel: " + b.durabilityLevel());
		s.append(", persistTo: " + b.persistTo());
		s.append(", replicateTo: " + b.replicateTo());
		s.append(", timeout: " + b.timeout());
		s.append(", retryStrategy: " + b.retryStrategy());
		s.append(", clientContext: " + b.clientContext());
		s.append(", parentSpan: " + b.parentSpan());
		s.append("}");
		return s.toString();
	}

	public static String toString(UpsertOptions o) {
		StringBuilder s = new StringBuilder();
		UpsertOptions.Built b = o.build();
		s.append("{");
		s.append("durabilityLevel: " + b.durabilityLevel());
		s.append(", persistTo: " + b.persistTo());
		s.append(", replicateTo: " + b.replicateTo());
		s.append(", timeout: " + b.timeout());
		s.append(", retryStrategy: " + b.retryStrategy());
		s.append(", clientContext: " + b.clientContext());
		s.append(", parentSpan: " + b.parentSpan());
		s.append("}");
		return s.toString();
	}

	public static String toString(ReplaceOptions o) {
		StringBuilder s = new StringBuilder();
		ReplaceOptions.Built b = o.build();
		s.append("{");
		s.append("cas: " + b.cas());
		s.append(", durabilityLevel: " + b.durabilityLevel());
		s.append(", persistTo: " + b.persistTo());
		s.append(", replicateTo: " + b.replicateTo());
		s.append(", timeout: " + b.timeout());
		s.append(", retryStrategy: " + b.retryStrategy());
		s.append(", clientContext: " + b.clientContext());
		s.append(", parentSpan: " + b.parentSpan());
		s.append("}");
		return s.toString();
	}

	public static String toString(RemoveOptions o) {
		StringBuilder s = new StringBuilder();
		RemoveOptions.Built b = o.build();
		s.append("{");
		s.append("cas: " + b.cas());
		s.append(", durabilityLevel: " + b.durabilityLevel());
		s.append(", persistTo: " + b.persistTo());
		s.append(", replicateTo: " + b.replicateTo());
		s.append(", timeout: " + b.timeout());
		s.append(", retryStrategy: " + b.retryStrategy());
		s.append(", clientContext: " + b.clientContext());
		s.append(", parentSpan: " + b.parentSpan());
		s.append("}");
		return s.toString();
	}

	public static String toString(MutateInOptions o) {
		StringBuilder s = new StringBuilder();
		MutateInOptions.Built b = o.build();
		s.append("{");
		s.append("cas: " + b.cas());
		s.append(", durabilityLevel: " + b.durabilityLevel());
		s.append(", persistTo: " + b.persistTo());
		s.append(", replicateTo: " + b.replicateTo());
		s.append(", timeout: " + b.timeout());
		s.append(", retryStrategy: " + b.retryStrategy());
		s.append(", clientContext: " + b.clientContext());
		s.append(", parentSpan: " + b.parentSpan());
		s.append("}");
		return s.toString();
	}
}
