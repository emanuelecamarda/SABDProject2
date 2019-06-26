package utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class ObjCounter<T> implements Serializable {

    private static final long serialVersionUID = 4858185737378394432L;

    private final Map<T, Long> objToCounts = new HashMap<>();

    public ObjCounter() {
    }

    public void incrementCount(T obj) {
        Long count = objToCounts.get(obj);
        if (count == null) {
            count = new Long(0);
            objToCounts.put(obj, count);
        }
        count++;
    }

    public long getCount(T obj) {
        Long count = objToCounts.get(obj);
        if (count == null) {
            return 0;
        } else {
            return count;
        }
    }

    public Map<T, Long> getCounts() {
        return this.objToCounts;
    }

    public void wipeAll() {
        for (T obj : objToCounts.keySet()) {
            resetCountToZero(obj);
        }
    }

    private void resetCountToZero(T obj) {
        objToCounts.replace(obj, Long.valueOf(0));
    }

    private boolean shouldBeRemovedFromCounter(T obj) {
        return getCount(obj) == 0;
    }

    public void wipeZeros() {
        Set<T> objToBeRemoved = new HashSet<>();
        for (T obj : objToCounts.keySet()) {
            if (shouldBeRemovedFromCounter(obj)) {
                objToBeRemoved.add(obj);
            }
        }
        for (T obj : objToBeRemoved) {
            objToCounts.remove(obj);
        }
    }

}
