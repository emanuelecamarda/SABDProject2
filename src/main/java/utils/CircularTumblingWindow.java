package utils;

import java.io.Serializable;

public class CircularTumblingWindow implements Serializable {

    private int windowLengthInHours;
    private long startTimestamp;
    private long endTimestamp;
    private int maxWindowSlot;
    private int slot;

    public CircularTumblingWindow(int windowLengthInHours, long startTimestamp, int maxWindowSlot) {
        this.windowLengthInHours = windowLengthInHours;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = startTimestamp + windowLengthInHours * 60 * 60;
        this.maxWindowSlot = maxWindowSlot;
        this.slot = 0;
    }

    public int getWindowLengthInHours() {
        return windowLengthInHours;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public int getSlot() {
        return slot;
    }

    public void moveForward() {
        this.slot = (slot + 1) % maxWindowSlot;
        this.startTimestamp = endTimestamp;
        this.endTimestamp += windowLengthInHours * 60 * 60;
    }

}
