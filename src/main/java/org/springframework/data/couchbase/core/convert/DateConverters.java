/*
 * Copyright 2012-2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.convert;

import static java.time.ZoneId.systemDefault;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.util.ClassUtils;

/**
 * Out of the box conversions for java dates and calendars.
 *
 * @author Michael Nitschinger
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 */
public final class DateConverters {

  private DateConverters() {
  }

  private static final boolean JODA_TIME_IS_PRESENT = ClassUtils.isPresent("org.joda.time.LocalDate", null);

  /**
   * Returns all converters by this class that can be registered.
   *
   * @return the list of converters to register.
   */
  public static Collection<Converter<?, ?>> getConvertersToRegister() {
    List<Converter<?, ?>> converters = new ArrayList<Converter<?, ?>>();

    boolean useISOStringConverterForDate = Boolean.parseBoolean(
            System.getProperty("org.springframework.data.couchbase.useISOStringConverterForDate", "false"));

    if (useISOStringConverterForDate) {
      converters.add(DateToStringConverter.INSTANCE);
    } else {
      converters.add(DateToLongConverter.INSTANCE);
    }

    converters.add(SerializedObjectToDateConverter.INSTANCE);

    converters.add(CalendarToLongConverter.INSTANCE);
    converters.add(NumberToCalendarConverter.INSTANCE);

    if (JODA_TIME_IS_PRESENT) {
      converters.add(LocalDateToLongConverter.INSTANCE);
      converters.add(LocalDateTimeToLongConverter.INSTANCE);
      converters.add(DateTimeToLongConverter.INSTANCE);
      converters.add(NumberToLocalDateConverter.INSTANCE);
      converters.add(NumberToLocalDateTimeConverter.INSTANCE);
      converters.add(NumberToDateTimeConverter.INSTANCE);
    }

    return converters;
  }

  @WritingConverter
  public enum DateToStringConverter implements Converter<Date, String> {
    INSTANCE;

    @Override
    public String convert(Date source) {
      return source == null ? null : source.toInstant().toString();
    }
  }

  @ReadingConverter
  public enum SerializedObjectToDateConverter implements Converter<Object, Date> {
    INSTANCE;

    @Override
    public Date convert(Object source) {
      if (source == null) {
        return null;
      }
      if (source instanceof Number) {
        Date date = new Date();
        date.setTime(((Number) source).longValue());
        return date;
      } else if (source instanceof String) {
        return Date.from(Instant.parse((String) source).atZone(systemDefault()).toInstant());
      } else {
        //Unsupported serialized object
        return null;
      }
    }
  }

  @WritingConverter
  public enum DateToLongConverter implements Converter<Date, Long> {
    INSTANCE;

    @Override
    public Long convert(Date source) {
      return source == null ? null : source.getTime();
    }
  }

  @WritingConverter
  public enum CalendarToLongConverter implements Converter<Calendar, Long> {
    INSTANCE;

    @Override
    public Long convert(Calendar source) {
      return source == null ? null : source.getTimeInMillis() / 1000;
    }
  }

  @ReadingConverter
  public enum NumberToCalendarConverter implements Converter<Number, Calendar> {
    INSTANCE;

    @Override
    public Calendar convert(Number source) {
      if (source == null) {
        return null;
      }

      Calendar calendar = Calendar.getInstance();
      calendar.setTimeInMillis(source.longValue() * 1000);
      return calendar;
    }
  }

  @WritingConverter
  public enum LocalDateToLongConverter implements Converter<LocalDate, Long> {
    INSTANCE;

    @Override
    public Long convert(LocalDate source) {
      return source == null ? null : source.toDate().getTime();
    }
  }

  @WritingConverter
  public enum LocalDateTimeToLongConverter implements Converter<LocalDateTime, Long> {
    INSTANCE;

    @Override
    public Long convert(LocalDateTime source) {
      return source == null ? null : source.toDate().getTime();
    }
  }

  @WritingConverter
  public enum DateTimeToLongConverter implements Converter<DateTime, Long> {
    INSTANCE;

    @Override
    public Long convert(DateTime source) {
      return source == null ? null : source.toDate().getTime();
    }
  }

  @ReadingConverter
  public enum NumberToLocalDateConverter implements Converter<Number, LocalDate> {
    INSTANCE;

    @Override
    public LocalDate convert(Number source) {
      return source == null ? null : new LocalDate(source.longValue());
    }
  }

  @ReadingConverter
  public enum NumberToLocalDateTimeConverter implements Converter<Number, LocalDateTime> {
    INSTANCE;

    @Override
    public LocalDateTime convert(Number source) {
      return source == null ? null : new LocalDateTime(source.longValue());
    }
  }

  @ReadingConverter
  public enum NumberToDateTimeConverter implements Converter<Number, DateTime> {
    INSTANCE;

    @Override
    public DateTime convert(Number source) {
      return source == null ? null : new DateTime(source.longValue());
    }
  }

}
