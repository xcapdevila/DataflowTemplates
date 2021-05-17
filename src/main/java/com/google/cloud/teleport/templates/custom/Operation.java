package com.google.cloud.teleport.templates.custom;

import java.util.Arrays;

/**
 * @author Xavier Capdevila Estevez on 12/5/21.
 */
public enum Operation {

  ADD("+"),
  SUB("-");

  private final String symbol;

  Operation(String symbol) {
    this.symbol = symbol;
  }

  public static Operation getBySymbol(String symbol) {
    return Arrays
        .stream(Operation.values())
        .filter(operation -> operation.symbol.equalsIgnoreCase(symbol))
        .findFirst()
        .orElseThrow(UnsupportedOperationException::new);
  }

}
