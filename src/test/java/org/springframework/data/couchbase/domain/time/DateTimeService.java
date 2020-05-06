package org.springframework.data.couchbase.domain.time;

import java.time.ZonedDateTime;

public interface DateTimeService {
	ZonedDateTime getCurrentDateAndTime();
}
