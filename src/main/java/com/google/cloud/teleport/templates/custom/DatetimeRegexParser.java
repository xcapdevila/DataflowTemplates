package com.google.cloud.teleport.templates.custom;

import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 * Utility class that implements custom Datetime related functions.
 *
 *  Available "functions":
 * - NOW for string fields
 * - DNOW for date fields
 *
 * Supports queries with custom NOW/DNOW functions:
 * - SELECT * FROM `Kind` WHERE string_field < ${NOW-12h}
 * - SELECT * FROM `Kind` WHERE string_field < ${NOW-2y} AND string_field > ${NOW-5y}
 * - SELECT * FROM `Kind` WHERE date_field < ${DNOW-2y}
 * - SELECT * FROM `Kind` WHERE date_field < ${DNOW-2y} AND date_field > ${DNOW-5y}
 * - SELECT * FROM `Kind` WHERE date_field < ${DNOW-2y} AND string_field > ${NOW-5y}
 *
 * NOW format is ${NOW_Operation__Value__TimeUnit_}.
 *
 * _Operation_:
 * - "+" to add
 * - "-" to subtract
 * _Value_:
 * - any integer greater than zero.
 * _TimeUnit_:
 * - "y" for years
 * - "m" for months
 * - "d" for days
 * - "h" for hours
 * </pre>
 *
 * @author Xavier Capdevila Estevez on 17/5/21.
 */
public class DatetimeRegexParser {

  public static final String STRING_NOW_REPLACEMENT_MASK = "'%s'";
  public static final String DATETIME_NOW_REPLACEMENT_MASK = "DATETIME('%sZ')";
  private static final Logger LOG = LoggerFactory.getLogger(DatastoreToDatastoreDeleteCurrentDate.class);
  private static final String STRING_NOW_REGEX = "\\$\\{NOW(\\+|\\-)(\\d*?)(y|m|d|h)\\}";
  private static final Pattern STRING_NOW_PATTERN = Pattern.compile(STRING_NOW_REGEX);
  private static final String DATETIME_NOW_REGEX = "\\$\\{DNOW(\\+|\\-)(\\d*?)(y|m|d|h)\\}";
  private static final Pattern DATETIME_NOW_PATTERN = Pattern.compile(DATETIME_NOW_REGEX);

  public static String process(String statement, LocalDateTime now) {
    statement = parse(statement, STRING_NOW_PATTERN, STRING_NOW_REPLACEMENT_MASK, now);
    statement = parse(statement, DATETIME_NOW_PATTERN, DATETIME_NOW_REPLACEMENT_MASK, now);
    return statement;
  }

  private static String parse(String statement, Pattern pattern, String replacementMask, LocalDateTime now) {
    final Matcher matcher = pattern.matcher(statement);
    while (matcher.find()) {
      final String parsedDate = DatetimeHelper
          .create()
          .withNow(now)
          .withOperation(matcher.group(1))
          .withValue(matcher.group(2))
          .withTimeUnit(matcher.group(3))
          .build()
          .calculate();
      LOG.info("parsedDate: {}", parsedDate);

      final String replacement = String.format(replacementMask, parsedDate);
      LOG.info("replacement: {}", replacement);
      statement = statement.replaceFirst(pattern.pattern(), replacement);
      LOG.info("statement: {}", statement);
    }
    return statement;
  }

}
