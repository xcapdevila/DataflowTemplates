package com.google.cloud.teleport.templates.custom;

import java.time.LocalDateTime;

/**
 * @author Xavier Capdevila Estevez on 12/5/21.
 */
public class DatetimeHelper {

  private final LocalDateTime now;
  private final Operation operation;
  private final Long value;
  private final TimeUnit timeUnit;

  private DatetimeHelper(LocalDateTime now, Operation operation, Long value, TimeUnit timeUnit) {
    this.now = now;
    this.operation = operation;
    this.value = value;
    this.timeUnit = timeUnit;
  }

  public static DatetimeHelperBuilder create() {
    return new DatetimeHelperBuilder();
  }

  public String calculate() {
    LocalDateTime parsedDatetime;
    if (operation == Operation.ADD) {
      parsedDatetime = now.plus(value, timeUnit.getChronoUnit());
    } else if (operation == Operation.SUB) {
      parsedDatetime = now.minus(value, timeUnit.getChronoUnit());
    } else {
      throw new UnsupportedOperationException("Not implemented");
    }
    return parsedDatetime.toString();
  }

  /**
   * @author Xavier Capdevila Estevez on 12/5/21.
   */
  public static final class DatetimeHelperBuilder {

    private LocalDateTime now;
    private Operation operation;
    private Long value;
    private TimeUnit timeUnit;

    private DatetimeHelperBuilder() {
    }

    public DatetimeHelperBuilder withNow(LocalDateTime now) {
      this.now = now;
      return this;
    }

    public DatetimeHelperBuilder withOperation(String operationSymbol) {
      this.operation = Operation.getBySymbol(operationSymbol);
      return this;
    }

    public DatetimeHelperBuilder withValue(String value) {
      this.value = Long.parseLong(value);
      return this;
    }

    public DatetimeHelperBuilder withTimeUnit(String timeUnitSymbol) {
      this.timeUnit = TimeUnit.getBySymbol(timeUnitSymbol);
      return this;
    }

    public DatetimeHelper build() {
      return new DatetimeHelper(now, operation, value, timeUnit);
    }
  }

}

