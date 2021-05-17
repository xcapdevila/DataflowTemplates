package com.google.cloud.teleport.templates.custom;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;

/**
 * @author Xavier Capdevila Estevez on 12/5/21.
 */
public enum TimeUnit {

  YEARS("y", ChronoUnit.YEARS),
  MONTHS("m", ChronoUnit.MONTHS),
  DAYS("d", ChronoUnit.DAYS),
  HOURS("h", ChronoUnit.HOURS);

  private final String symbol;
  private final ChronoUnit chronoUnit;

  TimeUnit(String symbol, ChronoUnit chronoUnit) {
    this.symbol = symbol;
    this.chronoUnit = chronoUnit;
  }

  public ChronoUnit getChronoUnit() {
    return chronoUnit;
  }

  public static TimeUnit getBySymbol(String symbol) {
    return Arrays
        .stream(TimeUnit.values())
        .filter(timeUnit -> timeUnit.symbol.equalsIgnoreCase(symbol))
        .findFirst()
        .orElseThrow(UnsupportedOperationException::new);
  }

}
