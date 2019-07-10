import java.util.GregorianCalendar;

public class Test {

    public static void main(String[] args) throws Exception {
        GregorianCalendar start = new GregorianCalendar(2018, 01, 01);
        System.out.println(start + "\n" + start.getTimeInMillis() / 1000);
        GregorianCalendar gc = new GregorianCalendar();
        gc.setTimeInMillis(1514767661 * 1000);
        System.out.println(gc + "\n" + gc.getTimeInMillis() / 1000);
    }
}
