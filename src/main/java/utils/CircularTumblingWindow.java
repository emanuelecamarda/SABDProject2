package utils;

import java.io.Serializable;

public class CircularTumblingWindow implements Serializable {

    private int windowLenghtInHours;
    private long startTimestamp;
    private long endTimestamp;
    private int maxWindowSlot;
    private int slot;

    public CircularTumblingWindow(int windowLenghtInHours, long startTimestamp, int maxWindowSlot) {
        this.windowLenghtInHours = windowLenghtInHours;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = startTimestamp + windowLenghtInHours * 60 * 60;
        this.maxWindowSlot = maxWindowSlot;
        this.slot = 0;
    }

    public int getWindowLenghtInHours() {
        return windowLenghtInHours;
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
        this.endTimestamp += windowLenghtInHours * 60 * 60;
    }

}
