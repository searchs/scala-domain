package com.ohprice.nlytics.utils

import java.util

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model._
import org.joda.time.DateTime


/**
 * My allo world for AWS SQS Queues!
 */
object SQSApplication with SourceReader {
  private var credentials = new BasicAWSCredentials("_", "_")

  def main(args: Array[String]): Unit = {
    println("=" * 55 + "\n")
    println("SQS Implementations")
    val amazonSQS = AmazonSQSClientBuilder.standard.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.EU_WEST_2).build
    val currentQueueList = amazonSQS.listQueues
    //        Create a standard queue
    val newQueueName = "plc-messages"
    if (currentQueueList.getQueueUrls.stream.noneMatch((l: String) => l.contains(newQueueName))) {
      val createStandardQueueRequest = new CreateQueueRequest().withQueueName(newQueueName)
      //
      val standardQueueUrl = amazonSQS.createQueue(createStandardQueueRequest).getQueueUrl
      //            System.out.println(standardQueueUrl);
      //            System.out.println(amazonSQS.listQueues());
    }
    else {
      println("\nQueue " + newQueueName + " already exists! see the list below.")
      println(amazonSQS.getQueueUrl(newQueueName))
      //            amazonSQS.purgeQueue(new PurgeQueueRequest());
    }
    //        POST MESSAGE
    val standardQueueUrl = amazonSQS.getQueueUrl(newQueueName).getQueueUrl
    val messageAttributes = new util.HashMap[String, MessageAttributeValue]
    messageAttributes.put("AttributeOne", new MessageAttributeValue().withStringValue("This is One attribute").withDataType("String"))
    for (i <- 0 until 20) {
      val sendMessageRequest = new SendMessageRequest().withQueueUrl(standardQueueUrl)
        .withMessageBody("FisyBobo Rolling Box: " + "msg" + i + " " + new DateTime().getMillis() + i + " " + new DateTime().getMillis)
        .withDelaySeconds(3).withMessageAttributes(messageAttributes)
      amazonSQS.sendMessage(sendMessageRequest)
    }
    val receiveMessageRequest = new ReceiveMessageRequest(standardQueueUrl).withWaitTimeSeconds(15).withMaxNumberOfMessages(10)
    val sqsMessages = amazonSQS.receiveMessage(receiveMessageRequest).getMessages
    System.out.println(sqsMessages.get(0))
    //        System.out.println(sqsMessages.stream().filter(l -> l.getBody().contains("msg1")).count());
  }



}

