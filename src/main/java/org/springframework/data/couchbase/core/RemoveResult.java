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
