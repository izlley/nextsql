package org.apache.nextsql.util;

import java.util.concurrent.atomic.AtomicLong;

public class SequentialNumber {
  private final AtomicLong _currentVal;
  
  public SequentialNumber(final long initialValue) {
    _currentVal = new AtomicLong(initialValue);
  }
  
  public long getCurrentValue() {
    return _currentVal.get();
  }
  
  public void setCurrentValue(long value) {
    _currentVal.set(value);
  }
  
  public long nextValue() {
    return _currentVal.incrementAndGet();
  }
  
  public void skipTo(long newValue) throws IllegalStateException {
    for (;;) {
      final long c = getCurrentValue();
      if (newValue < c) {
        throw new IllegalStateException(
            "Cannot skip to less than the current value (="
            + c + "), where newValue=" + newValue);
      }
      if (_currentVal.compareAndSet(c, newValue)) {
        return;
      }
    }
  }
  
  public boolean equals(final Object that) {
    if (that == null || this.getClass() != that.getClass()) {
      return false;
    }
    final AtomicLong thatVal = ((SequentialNumber)that)._currentVal;
    return _currentVal.equals(thatVal);
  }
  
  public int hashCode() {
    final long v = _currentVal.get();
    return (int)v ^ (int)(v >>> 32);
  }
}
