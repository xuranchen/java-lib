package com.wavefront.data;

/**
 * Base class for all parsing-related exceptions.
 *
 * @author vasily@wavefront.com
 */
public class ParseException extends IllegalArgumentException {
  public ParseException(String message) {
    super(message);
  }
}
