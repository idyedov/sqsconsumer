package com.dyedov.sqs.consumer;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * SQS Consumer Runnable meant to be executed from a single thread.
 *
 * Example usage:
 *
 *     private static final int NUM_LISTENER_THREADS = 5;
 *
 *     public static void main(String[] args) {
 *         ExecutorService executorService = Executors.newFixedThreadPool(NUM_LISTENER_THREADS);
 *
 *         for (int i = 0; i < NUM_LISTENER_THREADS; ++i) {
 *             executorService.submit(SQSConsumer.builder()
 *                     .withMaxNumberOfMessages(10)
 *                     .withSqs(AmazonSQSClientBuilder.defaultClient())
 *                     .withMessageProcessor(m -> System.out.println("received message: " + m))
 *                     .withBackOff(new SQSMessageWaitTimeFunction(
 *                         new ExponentialBackoffStrategy(),
 *                         Duration.ofMinutes(1),
 *                         Duration.ofHours(1)
 *                     ))
 *                     .withQueueName("TestQueue")
 *                     .build());
 *         }
 *     }
 */
public class SQSConsumer implements Runnable {
    private final AmazonSQS sqs;
    private final String queueUrl;
    private final ReceiveMessageRequest receiveMessageRequest;
    private final Consumer<Message> messageProcessor;
    private final Duration sleepBetweenPolls;
    private final Optional<Function<Message, Integer>> visibilityTimeoutProvider;

    private volatile boolean shutdown;

    public static class Builder {
        private AmazonSQS sqs;
        private final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                // needed for backoff strategy
                .withAttributeNames(SQSConstants.APPROXIMATE_RECEIVE_COUNT, SQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP);
        private Consumer<Message> messageProcessor;
        private Function<Message, Integer> backOff = null;
        private Duration sleepBetweenPolls = Duration.ZERO;

        /**
         * Set the SQS client.
         * @param sqs the SQS client.
         * @return builder
         */
        public Builder withSqs(AmazonSQS sqs) {
            this.sqs = sqs;
            return this;
        }

        /**
         * Set the SQS queue URL.
         * @param queueUrl the SQS queue URL.
         * @return builder
         */
        public Builder withQueueURL(String queueUrl) {
            receiveMessageRequest.setQueueUrl(queueUrl);
            return this;
        }

        /**
         * Set the message processor to use.
         * @param messageProcessor the message processor.
         * @return builder
         */
        public Builder withMessageProcessor(Consumer<Message> messageProcessor) {
            this.messageProcessor = messageProcessor;
            return this;
        }

        /**
         * Set the back-off strategy.
         * @param backOff the back-off strategy.
         * @return builder
         */
        public Builder withBackOff(Function<Message, Integer> backOff) {
            this.backOff = backOff;
            return this;
        }

        /**
         * Set the duration of time to sleep between each poll.
         * @param sleepBetweenPolls the Duration of time to sleep between each poll.
         * @return builder
         */
        public Builder withSleepBetweenPolls(Duration sleepBetweenPolls) {
            this.sleepBetweenPolls = sleepBetweenPolls;
            return this;
        }

        /**
         * See {@link com.amazonaws.services.sqs.model.ReceiveMessageRequest#setMaxNumberOfMessages(Integer)}.
         * @return builder
         */
        public Builder withMaxNumberOfMessages(Integer maxNumberOfMessages) {
            this.receiveMessageRequest.setMaxNumberOfMessages(maxNumberOfMessages);
            return this;
        }

        /**
         * See {@link com.amazonaws.services.sqs.model.ReceiveMessageRequest#setWaitTimeSeconds(Integer)}.
         * @return builder
         */
        public Builder withWaitTimeSeconds(Integer waitTimeSeconds) {
            this.receiveMessageRequest.setWaitTimeSeconds(waitTimeSeconds);
            return this;
        }

        /**
         * See {@link com.amazonaws.services.sqs.model.ReceiveMessageRequest#setMessageAttributeNames(Collection)}.
         * @return builder
         */
        public Builder withMessageAttributeNames(Collection<String> messageAttributeNames) {
            this.receiveMessageRequest.setMessageAttributeNames(messageAttributeNames);
            return this;
        }

        /**
         * See {@link com.amazonaws.services.sqs.model.ReceiveMessageRequest#setVisibilityTimeout(Integer)}.
         * @return builder
         */
        public Builder withVisibilityTimeout(Integer visibilityTimeout) {
            this.receiveMessageRequest.setVisibilityTimeout(visibilityTimeout);
            return this;
        }

        /**
         * Constructs a valid SQSConsumer instance.
         * @return SQSConsumer instance.
         */
        public SQSConsumer build() {
            if (sqs == null) {
                throw new IllegalArgumentException("sqs is required");
            }
            if (messageProcessor == null) {
                throw new IllegalArgumentException("messageProcessor is required");
            }
            if (receiveMessageRequest.getQueueUrl() == null) {
                throw new IllegalArgumentException("queueUrl is required");
            }
            if (sleepBetweenPolls.isNegative()) {
                throw new IllegalArgumentException("sleepBetweenPolls has to be greater than 0");
            }
            return new SQSConsumer(
                    sqs,
                    receiveMessageRequest,
                    messageProcessor,
                    sleepBetweenPolls,
                    Optional.ofNullable(backOff)
            );
        }
    }

    /**
     * Default SQSConsumer builder.
     * @return SQSConsumer.Builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Default constructor
     */
    private SQSConsumer(
            AmazonSQS sqs, ReceiveMessageRequest receiveMessageRequest, Consumer<Message> messageProcessor,
            Duration sleepBetweenPolls, Optional<Function<Message, Integer>> visibilityTimeoutProvider
    ) {
        this.sqs = sqs;
        this.queueUrl = receiveMessageRequest.getQueueUrl();
        this.receiveMessageRequest = receiveMessageRequest;
        this.messageProcessor = messageProcessor;
        this.sleepBetweenPolls = sleepBetweenPolls;
        this.visibilityTimeoutProvider = visibilityTimeoutProvider;
    }

    /**
     * Run the SQS Consumer.
     */
    @Override
    public void run() {
        while (true) {
            ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
            List<Message> receivedMessages = receiveMessageResult.getMessages();
            processMessageBatch(receivedMessages);
            if (!sleepBetweenPolls.isZero()) {
                try {
                    Thread.sleep(sleepBetweenPolls.toMillis());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (shutdown) {
                return;
            }
        }
    }

    /**
     * Request for SQS consumer to shut down.
     */
    public void shutdown() {
        shutdown = true;
    }

    private void processMessageBatch(List<Message> messages) {
        List<Message> successfulMessages = new ArrayList<>(messages.size());
        List<Message> failedMessages = new ArrayList<>(messages.size());

        for (Message m: messages) {
            try {
                messageProcessor.accept(m);
                successfulMessages.add(m);
            } catch (Exception e) {
                failedMessages.add(m);
            }
        }

        if (!successfulMessages.isEmpty()) {
            processSuccessfulMessages(successfulMessages);
        }

        if (!failedMessages.isEmpty()) {
            processFailedMessages(failedMessages);
        }
    }

    private void processSuccessfulMessages(List<Message> messages) {
        List<DeleteMessageBatchRequestEntry> deleteMessageRequests = messages.stream()
                .map(m -> new DeleteMessageBatchRequestEntry(m.getMessageId(), m.getReceiptHandle()))
                .collect(Collectors.toList());
        sqs.deleteMessageBatch(new DeleteMessageBatchRequest(queueUrl, deleteMessageRequests));
    }

    private void processFailedMessages(List<Message> messages) {
        if (!visibilityTimeoutProvider.isPresent()) {
            return;
        }
        Function<Message, Integer> backoffFunction = visibilityTimeoutProvider.get();
        List<ChangeMessageVisibilityBatchRequestEntry> changeMessageVisibilityRequests = messages.stream()
                .map(m -> new ChangeMessageVisibilityBatchRequestEntry(m.getMessageId(), m.getReceiptHandle())
                        .withVisibilityTimeout(backoffFunction.apply(m)))
                .collect(Collectors.toList());
        sqs.changeMessageVisibilityBatch(new ChangeMessageVisibilityBatchRequest(queueUrl, changeMessageVisibilityRequests));
    }
}
