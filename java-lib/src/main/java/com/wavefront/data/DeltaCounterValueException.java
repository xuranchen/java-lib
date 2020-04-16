package com.wavefront.data;

/**
 * Non-positive delta counters can be handled as a special case and not count
 * against "blocked", if desired.
 *
 * @author vasily@wavefront.com
 */
public class DeltaCounterValueException extends DataValidationException {
  public DeltaCounterValueException(String message) {
    super(message);
  }
}
