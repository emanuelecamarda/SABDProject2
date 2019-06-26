package utils;

import nodes.FilterQ1Bolt;
import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

public final class TumblingWindowCounter<T> implements Serializable {

    private static final long serialVersionUID = -2645063988768785810L;
    private static final GregorianCalendar startDate = new GregorianCalendar(2018, Calendar.JANUARY, 1,
            0, 0, 0);

    private ObjCounter<T> objCounter;
    private final int windowLengthInHours;
    private GregorianCalendar batchFinalDate;

    public TumblingWindowCounter(int windowLengthInHours) {
        this.objCounter = new ObjCounter<>();
        this.windowLengthInHours = windowLengthInHours;
        this.batchFinalDate = startDate;
        this.updateWindow();
    }

    public void incrementCount(T obj) {
        objCounter.incrementCount(obj);
    }

    public Map<T, Long> getCountThenAdvanceWindow() {
        Map<T, Long> counts = objCounter.getCounts();
        objCounter.wipeZeros();
        objCounter.wipeAll();
        this.updateWindow();
        return counts;
    }

    public void updateWindow() {
        this.batchFinalDate.add(Calendar.HOUR, windowLengthInHours);
    }

    public boolean checkShiftWindow(Tuple tuple) {
        Long createTime = Long.parseLong(tuple.getStringByField(FilterQ1Bolt.F_CREATE_TIME));
        if (createTime < this.batchFinalDate.getTimeInMillis() / 1000)
            return false;
        return true;
    }

    public GregorianCalendar getBatchFinalDate() {
        return batchFinalDate;
    }
}
