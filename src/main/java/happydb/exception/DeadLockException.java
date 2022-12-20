package happydb.exception;

/**
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
public class DeadLockException extends RuntimeException{
    public DeadLockException() {
        super();
    }

    public DeadLockException(String message) {
        super(message);
    }

    public DeadLockException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeadLockException(Throwable cause) {
        super(cause);
    }

    protected DeadLockException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
