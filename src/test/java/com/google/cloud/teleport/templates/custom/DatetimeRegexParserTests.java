package com.google.cloud.teleport.templates.custom;

import java.time.LocalDateTime;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * SELECT * FROM `Kind` WHERE string_field > '2020-12-12T16:33:15.599927'
 * SELECT * FROM `Kind` WHERE date_field > DATETIME('2020-12-12T16:33:15.599927Z')
 *
 * @author Xavier Capdevila Estevez on 12/5/21.
 */
public class DatetimeRegexParserTests {

  private static final String NOW_PATTERN = "\\$\\{NOW(\\+|\\-)(\\d*?)(y|m|d|h)\\}";
  private static final String ALLOWED_OPERATIONS = "+-";
  private static final String ALLOWED_TIMEUNITS = "ymdh";

  @Test
  public void process() {
    final String nowRegex = "${NOW-5y}";
    final String dnowRegex = "${DNOW+5y}";
    final String gqlStatement = "SELECT * FROM `Kind` WHERE string_field < " + nowRegex + " AND date_field > " + dnowRegex;
    final LocalDateTime now = LocalDateTime.now();
    final String processedStatement = DatetimeRegexParser.process(gqlStatement, now);

    final int stringIndex = processedStatement.indexOf("'") + 1;
    Assertions.assertEquals(
        now.minusYears(5L).toString().substring(0, 4),
        processedStatement.substring(stringIndex, stringIndex + 4));

    final int datetimeIndex = processedStatement.indexOf("DATETIME('") + 10;
    Assertions.assertEquals(
        now.plusYears(5L).toString().substring(0, 4),
        processedStatement.substring(datetimeIndex, datetimeIndex + 4));

    Assertions.assertFalse(processedStatement.contains(nowRegex));
    Assertions.assertFalse(processedStatement.contains(dnowRegex));

  }

  @Test
  public void parseNowInternal() {
    String gqlStatement = "SELECT * FROM `Kind` WHERE string_field < \"${NOW-5y}\" AND string_field > \"${NOW+5y}\"";
    final String replacement = "Matched!";
    Assertions.assertTrue(gqlStatement.replaceAll(NOW_PATTERN, replacement).contains(replacement));

    final Pattern pattern = Pattern.compile(NOW_PATTERN);
    final Matcher matcher = pattern.matcher(gqlStatement);
    while (matcher.find()) {
      final String operation = matcher.group(1);
      final String value = matcher.group(2);
      final String timeUnit = matcher.group(3);

      Assertions.assertAll(
          () -> Assertions.assertTrue(ALLOWED_OPERATIONS.contains(operation)),
          () -> Assertions.assertTrue(StringUtils.isNumeric(value)),
          () -> Assertions.assertTrue(ALLOWED_TIMEUNITS.contains(timeUnit.toLowerCase(Locale.ROOT)))
      );

      final String parsedDate = DatetimeHelper
          .create()
          .withNow(LocalDateTime.now())
          .withOperation(operation)
          .withValue(value)
          .withTimeUnit(timeUnit)
          .build()
          .calculate();
      if (operation.equals("+")) {
        Assertions.assertEquals(LocalDateTime.now().plusYears(Long.parseLong(value)).toString().substring(0, 4), parsedDate.substring(0, 4));
      } else {
        Assertions.assertEquals(LocalDateTime.now().minusYears(Long.parseLong(value)).toString().substring(0, 4), parsedDate.substring(0, 4));
      }

      String previousGqlStatement = gqlStatement;
      gqlStatement = gqlStatement.replaceFirst(pattern.pattern(), parsedDate);
      Assertions.assertFalse(gqlStatement.equals(previousGqlStatement));
    }

    Assertions.assertFalse(gqlStatement.contains("${NOW"));
    Assertions.assertFalse(gqlStatement.contains("}"));
  }

}
