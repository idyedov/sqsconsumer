package com.dyedov.sqs.consumer;

import java.time.Duration;

/**
 * Utility class with constants.
 */
class SQSConstants {
    /**
     * Returns the number of times a message has been received from the queue but not deleted.
     */
    public static final String APPROXIMATE_RECEIVE_COUNT = "ApproximateReceiveCount";
    /**
     * Returns the time the message was first received from the queue (epoch time in milliseconds).
     */
    public static final String APPROXIMATE_FIRST_RECEIVE_TIMESTAMP = "ApproximateFirstReceiveTimestamp";
    /**
     *
     */
    public static final Duration MAX_VISIBILITY_TIMEOUT = Duration.ofHours(12);
}
