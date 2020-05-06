package org.springframework.data.couchbase.domain.time;

import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;

import org.springframework.data.auditing.DateTimeProvider;

public class AuditingDateTimeProvider implements DateTimeProvider {

	private DateTimeService dateTimeService = new FixedDateTimeService();

	public AuditingDateTimeProvider() {}

	public AuditingDateTimeProvider(DateTimeService dateTimeService) {
		this.dateTimeService = dateTimeService;
	}

	@Override
	public Optional<TemporalAccessor> getNow() {
		return Optional.of(Instant.ofEpochSecond(dateTimeService.getCurrentDateAndTime().toEpochSecond()));
	}
}
