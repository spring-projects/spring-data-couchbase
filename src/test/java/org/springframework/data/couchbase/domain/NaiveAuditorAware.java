package org.springframework.data.couchbase.domain;

import org.springframework.data.domain.AuditorAware;

import java.util.Optional;

// These are the classes that would be used for a real getCurrentAuditor() implementation
//import org.springframework.security.core.Authentication;
//import org.springframework.security.core.context.SecurityContextHolder;
//import org.springframework.security.core.userdetails.User;

/**
 * This class returns a string that represents the current user
 *
 * @author Michael Reiche
 * @since 3.0
 */
public class NaiveAuditorAware implements AuditorAware<String> {

	private Optional<String> auditor = Optional.of("auditor");

	@Override
	public Optional<String> getCurrentAuditor() {
		return auditor;
	}

	public void setAuditor(String auditor) {
		this.auditor = Optional.of(auditor);
	}
}