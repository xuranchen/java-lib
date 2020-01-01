package com.wavefront.common;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A thread-safe implementation of a basic ring buffer with an ability to evict values on overflow.
 *
 * @param <T> type of objects stored
 *
 * @author vasily@wavefont.com
 */
@ThreadSafe
public class SynchronizedEvictingRingBuffer<T> extends EvictingRingBuffer<T> {
  private static final long serialVersionUID = -7810502868262740390L;

  /**
   * @param capacity desired capacity.
   */
  public SynchronizedEvictingRingBuffer(int capacity) {
    super(capacity, false, null, false);
  }

  /**
   * @param capacity          desired capacity
   * @param throwOnOverflow   Disables auto-eviction on overflow. When full capacity is
   *                          reached, all subsequent append() operations would throw
   *                          {@link IllegalStateException} if this parameter is true,
   *                          or evict the oldest value if this parameter is false.
   */
  public SynchronizedEvictingRingBuffer(int capacity, boolean throwOnOverflow) {
    super(capacity, throwOnOverflow, null, false);
  }

  /**
   * @param capacity      desired capacity.
   * @param defaultValue  pre-fill the buffer with this default value.
   */
  public SynchronizedEvictingRingBuffer(int capacity, @Nullable T defaultValue) {
    super(capacity, false, defaultValue, true);
  }

  /**
   * @param capacity         desired capacity.
   * @param throwOnOverflow  disables auto-eviction on overflow. When full capacity is
   *                         reached, all subsequent append() operations would throw
   *                         {@link IllegalStateException} if this parameter is true,
   *                         or evict the oldest value if this parameter is false.
   * @param defaultValue     pre-fill the buffer with this default value.
   */
  public SynchronizedEvictingRingBuffer(int capacity,
                                        boolean throwOnOverflow,
                                        @Nullable T defaultValue) {
    super(capacity, throwOnOverflow, defaultValue, true);
  }

  @Override
  public int size() {
    synchronized (this) {
      return super.size();
    }
  }

  @Override
  public T get(int index) {
    synchronized (this) {
      return super.get(index);
    }
  }

  @Override
  public boolean add(T value) {
    synchronized (this) {
      return super.add(value);
    }
  }

  @Override
  public boolean offer(T value) {
    synchronized (this) {
      return super.offer(value);
    }
  }

  @Override
  public List<T> toList() {
    synchronized (this) {
      return super.toList();
    }
  }

  @Nonnull
  @Override
  public Object[] toArray() {
    synchronized (this) {
      return super.toArray();
    }
  }

  @Override
  public T remove() {
    synchronized (this) {
      return super.remove();
    }
  }

  @Override
  public T poll() {
    synchronized (this) {
      return super.poll();
    }
  }

  @Override
  public T element() {
    synchronized (this) {
      return super.element();
    }
  }

  @Override
  public T peek() {
    synchronized (this) {
      return super.peek();
    }
  }

  @Override
  public boolean isEmpty() {
    synchronized (this) {
      return super.isEmpty();
    }
  }

  @Override
  public boolean contains(Object o) {
    synchronized (this) {
      return super.contains(o);
    }
  }

  @Override
  public boolean containsAll(@Nonnull Collection<?> coll) {
    synchronized (this) {
      return super.containsAll(coll);
    }
  }

  @Override
  public boolean addAll(@Nonnull Collection<? extends T> coll) {
    synchronized (this) {
      return super.addAll(coll);
    }
  }

  @Override
  public boolean removeAll(@Nonnull Collection<?> coll) {
    synchronized (this) {
      return super.removeAll(coll);
    }
  }

  @Override
  public boolean retainAll(@Nonnull Collection<?> coll) {
    synchronized (this) {
      return super.retainAll(coll);
    }
  }

  @Override
  public void clear() {
    synchronized (this) {
      super.clear();
    }
  }

  @Override
  public String toString() {
    synchronized (this) {
      return super.toString();
    }
  }

  // Override default methods in Collection
  @Override
  public void forEach(Consumer<? super T> consumer) {
    synchronized (this) {
      super.forEach(consumer);
    }
  }

  @Override
  public boolean removeIf(Predicate<? super T> filter) {
    synchronized (this) {
      return super.removeIf(filter);
    }
  }

  @Override
  public boolean remove(Object o) {
    synchronized (this) {
      return super.remove(o);
    }
  }

  @Override
  public int hashCode() {
    synchronized (this) {
      return super.hashCode();
    }
  }

  @Override
  public boolean equals(Object obj) {
    synchronized (this) {
      return super.equals(obj);
    }
  }

  @Nonnull
  @Override
  public <U> U[] toArray(@Nonnull U[] a) {
    return super.toArray(a);
  }
}
