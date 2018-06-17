package com.dyedov.sqs.consumer;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import com.evanlennick.retry4j.backoff.ExponentialBackoffStrategy;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;

public class SQSConsumerTest {
    private static final Duration INITIAL_DELAY_BETWEEN_RETRIES = Duration.ofMinutes(1);
    private static final Duration MAXIMUM_WAIT_TIME = Duration.ofHours(1);
    private static final String QUEUE_URL = "TestQueueUrl";

    private AmazonSQS amazonSQS;
    private SQSConsumer sqsConsumer;
    private List<Message> processedMessages;
    private ReceiveMessageResult receiveMessageResult;
    private MessageProcessor messageProcessor;

    private class MessageProcessor implements Consumer<Message> {

        private Optional<Consumer<Message>> secondaryMessageProcessor = Optional.empty();

        @Override
        public void accept(Message message) {
            processedMessages.add(message);
            secondaryMessageProcessor.ifPresent(m -> m.accept(message));
        }

        public void setSecondaryMessageProcessor(Consumer<Message> messageProcessor) {
            this.secondaryMessageProcessor = Optional.of(messageProcessor);
        }
    }

    @Before
    public void setUp() {
        this.amazonSQS = Mockito.mock(AmazonSQS.class);
        this.processedMessages = new ArrayList<>();
        this.receiveMessageResult = new ReceiveMessageResult();
        this.messageProcessor = new MessageProcessor();
        Mockito.doReturn(receiveMessageResult).when(amazonSQS).receiveMessage(Matchers.any(ReceiveMessageRequest.class));
        this.sqsConsumer = SQSConsumer.builder()
                .withSqs(amazonSQS)
                .withMessageProcessor(messageProcessor)
                .withBackOff(new SQSMessageVisibilityTimeFunction(new ExponentialBackoffStrategy(), INITIAL_DELAY_BETWEEN_RETRIES, MAXIMUM_WAIT_TIME))
                .withQueueURL(QUEUE_URL)
                .build();
        sqsConsumer.shutdown(); // sets the shutdown flag so we will only do a single poll for messages
    }

    @After
    public void tearDown() {
        Mockito.verifyNoMoreInteractions(amazonSQS);
    }

    @Test
    public void shouldNotProcessMessagesWhenNoMessagesExist() {
        sqsConsumer.run();
        Mockito.verify(amazonSQS).receiveMessage((ReceiveMessageRequest) Matchers.any());
        Assert.assertEquals(0, processedMessages.size());
    }

    @Test
    public void shouldProcessAndDeleteMessagesWhenMessagesExist() {
        Message message1 = generateMessage(0);
        Message message2 = generateMessage(0);
        receiveMessageResult.setMessages(Arrays.asList(message1, message2));
        sqsConsumer.run();
        Mockito.verify(amazonSQS).receiveMessage((ReceiveMessageRequest) Matchers.any());
        ArgumentCaptor<DeleteMessageBatchRequest> deleteMessageBatchRequest = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(amazonSQS).deleteMessageBatch(deleteMessageBatchRequest.capture());
        Assert.assertEquals(2, deleteMessageBatchRequest.getValue().getEntries().size());
        Assert.assertEquals(2, processedMessages.size());
        Assert.assertEquals(message1, processedMessages.get(0));
        Assert.assertEquals(message2, processedMessages.get(1));
    }

    @Test
    public void shouldAdjustWaitTimeOnFailure() {
        Message message1 = generateMessage(0); // successful message
        Message message2 = generateMessage(5); // failed message with 5 retries => 16 minutes
        Message message3 = generateMessage(10); // failed message with 10 retries => 512 minutes

        receiveMessageResult.setMessages(Arrays.asList(message1, message2, message3));
        messageProcessor.setSecondaryMessageProcessor(m -> {
            if (m == message2 || m == message3) {
                throw new RuntimeException("failed to process message");
            }
        });
        sqsConsumer.run();
        Mockito.verify(amazonSQS).receiveMessage((ReceiveMessageRequest) Matchers.any());

        ArgumentCaptor<DeleteMessageBatchRequest> deleteMessageBatchRequest = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(amazonSQS).deleteMessageBatch(deleteMessageBatchRequest.capture());
        Assert.assertEquals(1, deleteMessageBatchRequest.getValue().getEntries().size());
        Assert.assertEquals(message1.getMessageId(), deleteMessageBatchRequest.getValue().getEntries().get(0).getId());

        ArgumentCaptor<ChangeMessageVisibilityBatchRequest> request = ArgumentCaptor.forClass(ChangeMessageVisibilityBatchRequest.class);
        Mockito.verify(amazonSQS).changeMessageVisibilityBatch(request.capture());
        Assert.assertEquals(2, request.getValue().getEntries().size());
        Assert.assertEquals(message2.getMessageId(), request.getValue().getEntries().get(0).getId());
        Assert.assertEquals(Duration.ofMinutes(16), Duration.ofSeconds(request.getValue().getEntries().get(0).getVisibilityTimeout()));

        Assert.assertEquals(message3.getMessageId(), request.getValue().getEntries().get(1).getId());
        // exponential backoff value is 512 minutes (8.53 hours) but our maximum wait time is 1 hour
        Assert.assertTrue(request.getValue().getEntries().get(1).getVisibilityTimeout() < MAXIMUM_WAIT_TIME.getSeconds());
    }

    private Message generateMessage(Integer receiveCount) {
        Message message = new Message();
        Map<String, String> messageAttributes = new HashMap<>();
        messageAttributes.put(SQSConstants.APPROXIMATE_RECEIVE_COUNT, receiveCount.toString());
        messageAttributes.put(SQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, Long.toString(Instant.now().toEpochMilli()));
        message.setAttributes(messageAttributes);
        message.setMessageId(UUID.randomUUID().toString());
        return message;
    }
}
