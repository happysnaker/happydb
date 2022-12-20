package happydb.common;

/**
 * @Author happysnaker
 * @Date 2022/11/16
 * @Email happysnaker@foxmail.com
 */
public class Debug {
    public static boolean debug = true;

    public static void log(String message, Object... args) {
        if (debug) {
            System.out.printf(message, args);
            System.out.println();
        }
    }

    public static void log(Object msg) {
        log(msg.toString(), (Object) null);
    }
}
