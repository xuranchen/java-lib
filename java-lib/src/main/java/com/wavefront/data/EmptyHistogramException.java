package com.wavefront.data;

/**
 * Empty histograms can be handled as a special case and not count against "blocked", if desired.
 *
 * @author vasily@wavefront.com
 */
public class EmptyHistogramException extends DataValidationException {
  public EmptyHistogramException(String message) {
    super(message);
  }
}
