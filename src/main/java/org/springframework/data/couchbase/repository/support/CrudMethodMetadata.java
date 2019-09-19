package org.springframework.data.couchbase.repository.support;

import java.lang.reflect.Method;

import org.springframework.data.couchbase.repository.ScanConsistency;

public interface CrudMethodMetadata {

	/**
	 * Returns the {@link Method} to be used.
	 */
	Method getMethod();

	/**
	 * If present holds the scan consistency annotation (null otherwise).
	 */
	ScanConsistency getScanConsistency();

}
