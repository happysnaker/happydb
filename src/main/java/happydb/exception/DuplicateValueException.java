package happydb.exception;

/**
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
public class DuplicateValueException extends Exception{
    public DuplicateValueException() {
        super();
    }

    public DuplicateValueException(String message) {
        super(message);
    }

    public DuplicateValueException(String message, Throwable cause) {
        super(message, cause);
    }

    public DuplicateValueException(Throwable cause) {
        super(cause);
    }

    protected DuplicateValueException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
