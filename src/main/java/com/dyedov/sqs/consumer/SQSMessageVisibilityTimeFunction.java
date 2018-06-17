package com.dyedov.sqs.consumer;

import com.amazonaws.services.sqs.model.Message;
import com.evanlennick.retry4j.backoff.BackoffStrategy;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

/**
 * Function to compute new visibility time for messages that failed to process.
 */
public class SQSMessageVisibilityTimeFunction implements Function<Message, Integer> {
    private final BackoffStrategy backoffStrategy;
    private final Duration initialDelayBetweenRetries;
    private final Duration maximumWaitTime;

    /**
     * Default constructor.
     * @param backoffStrategy the backoff strategy to use.
     * @param initialDelayBetweenRetries the initial delay Duration to use for backoff.
     * @param maximumWaitTime the maximum Duration to wait from first message receive timestamp.
     */
    public SQSMessageVisibilityTimeFunction(
            BackoffStrategy backoffStrategy, Duration initialDelayBetweenRetries, Duration maximumWaitTime
    ) {
        this.backoffStrategy = backoffStrategy;
        this.initialDelayBetweenRetries = initialDelayBetweenRetries;
        this.maximumWaitTime = maximumWaitTime;
    }

    /**
     * Generate an SQS message visibility time for given SQS Message.
     * @param message the SQS message.
     * @return Integer value for message visibility time. Will never exceed MAX_VISIBILITY_TIMEOUT value.
     */
    @Override
    public Integer apply(Message message) {
        int approximateReceiveCount = Integer.parseInt(getAttribute(message, SQSConstants.APPROXIMATE_RECEIVE_COUNT));
        Instant approximateFirstReceiveTime = Instant.ofEpochMilli(Long.parseLong(getAttribute(message, SQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP)));
        Instant maximumTime = approximateFirstReceiveTime.plus(maximumWaitTime);
        Duration nextDelay = backoffStrategy.getDurationToWait(approximateReceiveCount, initialDelayBetweenRetries);
        Instant now = Instant.now();
        Duration adjustedDelay = Collections.min(Arrays.asList(
                SQSConstants.MAX_VISIBILITY_TIMEOUT,
                nextDelay,
                Duration.between(now, maximumTime)
        ));

        return adjustedDelay.isNegative() ? 0 : Math.toIntExact(adjustedDelay.getSeconds());
    }

    /**
     * Utility method to retrieve a String attribute of a message.
     * @param message the SQS Message.
     * @param attributeName the String attribute name.
     * @return the String attribute value if it exists, null otherwise.
     */
    private static String getAttribute(Message message, String attributeName) {
        return message.getAttributes().get(attributeName);
    }
}
