# SQS Consumer
A simple SQS consumer with backoff strategy for failure processing.

# Example Usage
```java
int numListenerThreads = 5;
ExecutorService executorService = Executors.newFixedThreadPool(numListenerThreads);
AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

for (int i = 0; i < numListenerThreads; ++i) {
    executorService.submit(SQSConsumer.builder()
        .withMaxNumberOfMessages(10)
        .withSqs(sqs)
        .withMessageProcessor(m -> System.out.println("received message: " + m))
        .withBackOff(new SQSMessageWaitTimeFunction(
            new ExponentialBackoffStrategy(),
            Duration.ofMinutes(1), // initial wait time
            Duration.ofHours(1) // maximum wait time for a message across all retries
        ))
        .withQueueURL(sqs.getQueueUrl("TestQueue").getQueueUrl())
        .build()
    );
}
```
