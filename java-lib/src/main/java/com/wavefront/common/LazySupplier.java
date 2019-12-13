package com.wavefront.common;

import java.util.function.Supplier;

/**
 * Caching wrapper with lazy init for a {@code Supplier}
 *
 * @author vasily@wavefront.com
 */
public abstract class LazySupplier {

  /**
   * Lazy-initialize a {@code Supplier}
   *
   * @param supplier {@code Supplier} to lazy-initialize
   * @return lazy wrapped supplier
   */
  public static <T> Supplier<T> of(Supplier<T> supplier) {
    return new Supplier<T>() {
      private volatile T value = null;

      @Override
      public T get() {
        if (value == null) {
          synchronized (this) {
            if (value == null) {
              value = supplier.get();
            }
          }
        }
        return value;
      }
    };
  }
}
