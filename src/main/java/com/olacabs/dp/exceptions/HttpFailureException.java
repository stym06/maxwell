package com.olacabs.dp.exceptions;

/**
 * Generic HttpFailure Exception to handle all http exceptions
 *
 * @author abhijit.singh
 * @version 1.0
 * @Date 15/06/16
 */
public class HttpFailureException extends Exception {

    private String message = null;

    public HttpFailureException() {
        super();
    }

    public HttpFailureException(String message) {
        super(message);
        this.message = message;
    }

    public HttpFailureException(Throwable cause) {
        super(cause);
    }

    @Override
    public String toString() {
        return message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
