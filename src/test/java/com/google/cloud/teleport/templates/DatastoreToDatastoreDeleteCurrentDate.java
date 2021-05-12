package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.templates.custom.DatetimeHelper;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * SELECT * FROM `Kind` WHERE timestamp > "2020-12-12T16:33:15.599927"
 *
 * @author Xavier Capdevila Estevez on 12/5/21.
 */
public class DatastoreToDatastoreDeleteCurrentDate {

  private static final String NOW_PATTERN = "\\$\\{NOW(\\+|\\-)(\\d*?)(y|m|d|h)\\}";

  @Test
  public void testRegex() {
    String gqlStatement = "SELECT * FROM `Kind` WHERE timestamp < \"${NOW-5y}\" AND timestamp > \"${NOW-2m}\"";
    System.out.println(gqlStatement.replaceAll(NOW_PATTERN, "Matched!"));
    final String[] parts = Pattern.compile(NOW_PATTERN).split(gqlStatement);
    for (String part : parts) {
      System.out.println(part);
    }
    final Matcher matcher = Pattern.compile(NOW_PATTERN).matcher(gqlStatement);
    while (matcher.find()) {
      System.out.println(matcher.groupCount());
      for (int i = 0; i <= matcher.groupCount(); i++) {
        System.out.println(i);
        System.out.println(matcher.group(i));
      }

      final String parsedDate = DatetimeHelper
          .create()
          .withNow(LocalDateTime.now())
          .withOperation(matcher.group(1))
          .withValue(matcher.group(2))
          .withTimeUnit(matcher.group(3))
          .build()
          .calculate();
      System.out.println(
          parsedDate);

      gqlStatement = gqlStatement.replaceFirst(NOW_PATTERN, parsedDate);
      System.out.println(gqlStatement);
    }

    System.out.println(gqlStatement);
    Assertions.assertTrue(!gqlStatement.contains("${NOW"));
  }

}
