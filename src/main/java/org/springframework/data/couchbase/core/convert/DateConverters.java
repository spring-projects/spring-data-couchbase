/*
 * Copyright 2012-2015 the original author or authors
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

package org.springframework.data.couchbase.core.convert;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.joda.time.DateMidnight;
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

    converters.add(DateToLongConverter.INSTANCE);
    converters.add(CalendarToLongConverter.INSTANCE);
    converters.add(NumberToDateConverter.INSTANCE);
    converters.add(NumberToCalendarConverter.INSTANCE);

    if (JODA_TIME_IS_PRESENT) {
      converters.add(LocalDateToLongConverter.INSTANCE);
      converters.add(LocalDateTimeToLongConverter.INSTANCE);
      converters.add(DateTimeToLongConverter.INSTANCE);
      converters.add(DateMidnightToLongConverter.INSTANCE);
      converters.add(NumberToLocalDateConverter.INSTANCE);
      converters.add(NumberToLocalDateTimeConverter.INSTANCE);
      converters.add(NumberToDateTimeConverter.INSTANCE);
      converters.add(NumberToDateMidnightConverter.INSTANCE);
    }

    return converters;
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
  public enum NumberToDateConverter implements Converter<Number, Date> {
    INSTANCE;

    @Override
    public Date convert(Number source) {
      if (source == null) {
        return null;
      }

      Date date = new Date();
      date.setTime(source.longValue());
      return date;
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

  @WritingConverter
  public enum DateMidnightToLongConverter implements Converter<DateMidnight, Long> {
    INSTANCE;

    @Override
    public Long convert(DateMidnight source) {
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

  @ReadingConverter
  public enum NumberToDateMidnightConverter implements Converter<Number, DateMidnight> {
    INSTANCE;

    @Override
    public DateMidnight convert(Number source) {
      return source == null ? null : new DateMidnight(source.longValue());
    }
  }

}
