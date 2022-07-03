package edu.berkeley.cs186.database.common.iterator;

import java.util.Iterator;

/**
 * 具有回溯功能的迭代器接口，可以对迭代器打标记，重置迭代器时返回标记的地方。
 * <br>
 * A BacktrackingIterator supports marking a point in iteration, and resetting the
 * state of the iterator back to that mark. For example, if you had a backtracking
 * iterator with the values [1,2,3]:
 *
 * BackTrackingIterator<Integer> iter = new BackTrackingIteratorImplementation();
 * iter.next();     // returns 1
 * iter.next();     // returns 2
 * iter.markPrev(); // marks the previously returned value, 2
 * iter.next();     // returns 3
 * iter.hasNext();  // returns false
 * iter.reset();    // reset to the marked value (line 5)
 * iter.hasNext();  // returns true
 * iter.next();     // returns 2
 * iter.markNext(); // mark the value to be returned next, 3
 * iter.next();     // returns 3
 * iter.hasNext();  // returns false
 * iter.reset();    // reset to the marked value (line 11)
 * iter.hasNext();  // returns true
 * iter.next();     // returns 3
 */
public interface BacktrackingIterator<T> extends Iterator<T> {
    /**
     * markPrev() marks the last returned value of the iterator, which is the last
     * returned value of next().
     *
     * Calling markPrev() on an iterator that has not yielded a record yet,
     * or that has not yielded a record since the last reset() call does nothing.
     */
    void markPrev();

    /**
     * markNext() marks the next returned value of the iterator, which is the
     * value returned by the next call of next().
     *
     * Calling markNext() on an iterator that has no records left does nothing.
     */
    void markNext();

    /**
     * 将迭代器重置到最后标记的位置，对 {@link #next() next()} 的后续调用应返回被标记的值，如果没有被标记则不用做任何事。
     * <br>
     * reset() resets the iterator to the last marked location. The subsequent
     * call to next() should return the value that was marked. If nothing has
     * been marked, reset() does nothing. You may reset() to the same point as
     * many times as desired until a new mark is set.
     */
    void reset();
}

