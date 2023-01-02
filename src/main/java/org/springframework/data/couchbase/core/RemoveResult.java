/*
 * Copyright 2012-2023 the original author or authors
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

import java.util.Objects;
import java.util.Optional;

import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.java.kv.MutationResult;

public class RemoveResult {

	private final String id;
	private final long cas;
	private final Optional<MutationToken> mutationToken;

	public RemoveResult(String id, long cas, Optional<MutationToken> mutationToken) {
		this.id = id;
		this.cas = cas;
		this.mutationToken = mutationToken;
	}

	public static RemoveResult from(final String id, final MutationResult result) {
		return new RemoveResult(id, result.cas(), result.mutationToken());
	}

	public String getId() {
		return id;
	}

	public long getCas() {
		return cas;
	}

	public Optional<MutationToken> getMutationToken() {
		return mutationToken;
	}

	@Override
	public String toString() {
		return "RemoveResult{" + "id='" + id + '\'' + ", cas=" + cas + ", mutationToken=" + mutationToken + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		RemoveResult that = (RemoveResult) o;
		return cas == that.cas && Objects.equals(id, that.id) && Objects.equals(mutationToken, that.mutationToken);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, cas, mutationToken);
	}
}
