package com.wavefront.data;

/**
 * Base class for all data validation exceptions.
 *
 * @author vasily@wavefront.com
 */
public class DataValidationException extends IllegalArgumentException {
  public DataValidationException(String message) {
    super(message);
  }
}
