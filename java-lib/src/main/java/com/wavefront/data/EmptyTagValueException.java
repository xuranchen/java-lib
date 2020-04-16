package com.wavefront.data;

/**
 * Empty tag values can be handled as a special case, if desired.
 *
 * @author vasily@wavefront.com
 */
public class EmptyTagValueException extends DataValidationException {
  public EmptyTagValueException(String message) {
    super(message);
  }
}
