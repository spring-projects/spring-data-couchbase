package org.springframework.data.couchbase.transaction;

import java.util.Objects;

import org.springframework.data.annotation.Immutable;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.couchbase.transactions.TransactionQueryOptions;

@Immutable
public final class ClientSessionOptions {
	private final Boolean causallyConsistent;
	private final Boolean snapshot;
	private final TransactionQueryOptions defaultTransactionOptions;

	@Nullable
	public Boolean isCausallyConsistent() {
		return this.causallyConsistent;
	}

	@Nullable
	public Boolean isSnapshot() {
		return this.snapshot;
	}

	public TransactionQueryOptions getDefaultTransactionOptions() {
		return this.defaultTransactionOptions;
	}

	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o != null && this.getClass() == o.getClass()) {
			ClientSessionOptions that = (ClientSessionOptions) o;
			if (!Objects.equals(this.causallyConsistent, that.causallyConsistent)) {
				return false;
			} else if (!Objects.equals(this.snapshot, that.snapshot)) {
				return false;
			} else {
				return Objects.equals(this.defaultTransactionOptions, that.defaultTransactionOptions);
			}
		} else {
			return false;
		}
	}

	public int hashCode() {
		int result = this.causallyConsistent != null ? this.causallyConsistent.hashCode() : 0;
		result = 31 * result + (this.snapshot != null ? this.snapshot.hashCode() : 0);
		result = 31 * result + (this.defaultTransactionOptions != null ? this.defaultTransactionOptions.hashCode() : 0);
		return result;
	}

	public String toString() {
		return "ClientSessionOptions{causallyConsistent=" + this.causallyConsistent + "snapshot=" + this.snapshot
				+ ", defaultTransactionOptions=" + this.defaultTransactionOptions + '}';
	}

	public static ClientSessionOptions.Builder builder() {
		return new ClientSessionOptions.Builder();
	}

	public static ClientSessionOptions.Builder builder(ClientSessionOptions options) {
		Assert.notNull(options, "options");
		ClientSessionOptions.Builder builder = new ClientSessionOptions.Builder();
		builder.causallyConsistent = options.isCausallyConsistent();
		builder.snapshot = options.isSnapshot();
		builder.defaultTransactionOptions = options.getDefaultTransactionOptions();
		return builder;
	}

	private ClientSessionOptions(ClientSessionOptions.Builder builder) {
		if (builder.causallyConsistent != null && builder.causallyConsistent && builder.snapshot != null
				&& builder.snapshot) {
			throw new IllegalArgumentException("A session can not be both a snapshot and causally consistent");
		} else {
			this.causallyConsistent = builder.causallyConsistent == null && builder.snapshot != null ? !builder.snapshot
					: builder.causallyConsistent;
			this.snapshot = builder.snapshot;
			this.defaultTransactionOptions = builder.defaultTransactionOptions;
		}
	}

	// @NotThreadSafe
	public static final class Builder {
		private Boolean causallyConsistent;
		private Boolean snapshot;
		private TransactionQueryOptions defaultTransactionOptions;

		public ClientSessionOptions.Builder causallyConsistent(boolean causallyConsistent) {
			this.causallyConsistent = causallyConsistent;
			return this;
		}

		public ClientSessionOptions.Builder snapshot(boolean snapshot) {
			this.snapshot = snapshot;
			return this;
		}

		public ClientSessionOptions.Builder defaultTransactionOptions(TransactionQueryOptions defaultTransactionOptions) {
			Assert.notNull(defaultTransactionOptions, "defaultTransactionOptions");
			this.defaultTransactionOptions = defaultTransactionOptions;
			return this;
		}

		public ClientSessionOptions build() {
			return new ClientSessionOptions(this);
		}

		private Builder() {
			/* TODO this.defaultTransactionOptions = TransactionQueryOptions.builder().build();*/
		}
	}
}
