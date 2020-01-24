package com.wavefront.ingester;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A lightweight parser custom-tailored to suit all of our supported line protocols.
 *
 * @author vasily@wavefront.com
 */
public class StringParser {
  private static final String EQ_TOKEN = "=";
  private static final String WEIGHT_TOKEN = "#";

  private int currentIndex = 0;
  private final String input;
  private String peek = null;

  /**
   * @param input string to parse at instance creation
   */
  public StringParser(@Nonnull String input) {
    Preconditions.checkNotNull(input);
    this.input = input;
    this.currentIndex = 0;
  }

  /**
   * Retrieves the next available token, but does not advance the further, so multiple
   * calls to peek() return the same value. The value is cached so performance
   * penalty for multiple peek() calls is negligible.
   *
   * @return next available token or null if end of line is reached
   */
  @Nullable
  public String peek() {
    if (peek == null) {
      peek = advance();
    }
    return peek;
  }

  /**
   * Checks whether there are more tokens available in the string.
   *
   * @return true if more tokens available
   */
  public boolean hasNext() {
    return peek() != null;
  }

  /**
   * Retrieves the next available token and advances further.
   *
   * @return next available token or null if end of line is reached
   */
  @Nullable
  public String next() {
    String token = peek();
    peek = null;
    return token;
  }

  @Nullable
  private String advance() {
    while (currentIndex < input.length() && Character.isWhitespace(input.charAt(currentIndex))) {
      // skip whitespace if any
      currentIndex++;
    }
    if (currentIndex >= input.length()) return null;
    char currentChar = input.charAt(currentIndex);
    currentIndex++;
    if (currentChar == '\"' || currentChar == '\'') {
      return parseAsQuoted(currentChar);
    } else if (currentChar == '=') {
      return EQ_TOKEN;
    } else if (currentChar == '#') {
      return WEIGHT_TOKEN;
    } else {
      return parseAsNonQuoted();
    }
  }

  private String parseAsQuoted(char quoteChar) {
    int index = input.indexOf(quoteChar, currentIndex);
    if (index == -1) throw new RuntimeException("Unmatched quote character: (" + quoteChar + ")");
    int startIndex = currentIndex;
    currentIndex = index + 1;
    if (input.charAt(index - 1) != '\\') {
      // no escaped quotes, can return immediately
      return input.substring(startIndex, index);
    }
    StringBuilder unquoted = new StringBuilder(index - startIndex + 16);
    boolean escapedQuote = true;
    while (escapedQuote) {
      unquoted.append(input, startIndex, index - 1);
      unquoted.append(quoteChar);
      index = input.indexOf(quoteChar, currentIndex);
      startIndex = currentIndex;
      currentIndex = index + 1;
      if (index == -1) throw new RuntimeException("Unmatched quote character: (" + quoteChar + ")");
      escapedQuote = input.charAt(index - 1) == '\\';
    }
    return unquoted.append(input, startIndex, index).toString();
  }

  private String parseAsNonQuoted() {
    int indexOfSeparator = indexOfAnySeparator(input, currentIndex);
    int endOfToken = indexOfSeparator == -1 ? input.length() : indexOfSeparator;
    String result = input.substring(currentIndex - 1, endOfToken);
    currentIndex = endOfToken;
    return result;
  }

  private static int indexOfAnySeparator(String input, int startIndex) {
    for (int i = startIndex; i < input.length(); i++) {
      char ch = input.charAt(i);
      if (ch == ' ' || ch == '=' || ch == '\t') return i;
    }
    return -1;
  }
}
