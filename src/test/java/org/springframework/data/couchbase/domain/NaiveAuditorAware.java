package org.springframework.data.couchbase.domain;

import org.springframework.data.domain.AuditorAware;

import java.util.Optional;

//import org.springframework.security.core.Authentication;
//import org.springframework.security.core.context.SecurityContextHolder;
//import org.springframework.security.core.userdetails.User;

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