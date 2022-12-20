package happydb.exception;

/**
 * @Author happysnaker
 * @Date 2022/11/15
 * @Email happysnaker@foxmail.com
 */
public class TimeoutException extends Exception{
    public TimeoutException() {
        super();
    }

    public TimeoutException(String message) {
        super(message);
    }

    public TimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public TimeoutException(Throwable cause) {
        super(cause);
    }

    protected TimeoutException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
