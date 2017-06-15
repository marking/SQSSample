/*******************************************************************************
* Copyright 2009-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
* 
* Licensed under the Apache License, Version 2.0 (the "License"). You may
* not use this file except in compliance with the License. A copy of the
* License is located at
* 
* http://aws.amazon.com/apache2.0/
* 
* or in the "license" file accompanying this file. This file is
* distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the specific
* language governing permissions and limitations under the License.
*******************************************************************************/

using System;
using System.Collections.Generic;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft;
using Newtonsoft.Json;
using Common;
using System.Threading;

namespace SQSSample1
{

    class Program
    {
        public static void Main(string[] args)
        {
            var sqs = new AmazonSQSClient();

            try
            {
                Console.WriteLine("===========================================");
                Console.WriteLine("Getting Started with Amazon SQS");
                Console.WriteLine("===========================================\n");
                var callBackQueue = Guid.NewGuid().ToString("N");
                //Creating a queue
                Console.WriteLine($"Create a queue called {callBackQueue}.\n");
                var sqsRequest = new CreateQueueRequest { QueueName = callBackQueue };
                var createQueueResponse = sqs.CreateQueue(sqsRequest);
                string myQueueUrl = createQueueResponse.QueueUrl;

                //Confirming the queue exists
                var listQueuesRequest = new ListQueuesRequest();
                var listQueuesResponse = sqs.ListQueues(listQueuesRequest);

                Console.WriteLine("Printing list of Amazon SQS queues.\n");
                if (listQueuesResponse.QueueUrls != null)
                {
                    foreach (String queueUrl in listQueuesResponse.QueueUrls)
                    {
                        Console.WriteLine("  QueueUrl: {0}", queueUrl);
                    }
                }
                Console.WriteLine();
                var numMessages = 5;

                //Sending a message
                Console.WriteLine("Sending a message to MyQueue.\n");
                for (var i = 0; i < numMessages; i++)
                {
                    Thread.Sleep(300);
                    var msg = new Common.Message()
                    {
                        CallbackQueue = new Uri(myQueueUrl),
                        S3Path = $"s3://us-east-1/assembleassets/path/to/{callBackQueue}/dwfx_{i}.dwfx",
                        TypeOfMessage = MessageType.ProcessRequest,
                        TotalMessages = numMessages,
                        ThisMessage = i
                    };

                    var sendMessageRequest = new SendMessageRequest
                    {
                        QueueUrl = "https://queue.amazonaws.com/257724145439/NewDWFxFile.fifo", //Processing queue
                        MessageBody = JsonConvert.SerializeObject(msg, Formatting.None),
                        MessageGroupId = $"{callBackQueue}-{i}",
                        MessageDeduplicationId = $"{callBackQueue}-{i}"
                    };
                    sqs.SendMessage(sendMessageRequest);
                }
                //Receiving a message
                var totalReceived = 0;
                while (totalReceived < numMessages)
                { 
                    var receiveMessageRequest = new ReceiveMessageRequest
                    {
                        QueueUrl = myQueueUrl,
                        MaxNumberOfMessages = numMessages,
                        WaitTimeSeconds = 1,
                        AttributeNames = new List<string>() { "all"},
                        MessageAttributeNames = new List<string>( ) { "all"}
                    };
                    var receiveMessageResponse = sqs.ReceiveMessage(receiveMessageRequest);
                    if (receiveMessageResponse.Messages != null)
                    {
                        Console.WriteLine("Printing received message.\n");
                        foreach (var message in receiveMessageResponse.Messages)
                        {
                            Console.WriteLine("  Message");
                            foreach (var keyName in message.MessageAttributes.Keys)
                            {
                                Console.WriteLine($"    {keyName} -> {message.MessageAttributes[keyName]}");
                            }
                            foreach (var keyName in message.Attributes.Keys)
                            {
                                Console.WriteLine($"    {keyName} -> {message.Attributes[keyName]}");
                            }
                            if (!string.IsNullOrEmpty(message.MessageId))
                            {
                                Console.WriteLine("    MessageId: {0}", message.MessageId);
                            }
                            if (!string.IsNullOrEmpty(message.ReceiptHandle))
                            {
                                Console.WriteLine("    ReceiptHandle: {0}", message.ReceiptHandle);
                            }
                            if (!string.IsNullOrEmpty(message.MD5OfBody))
                            {
                                Console.WriteLine("    MD5OfBody: {0}", message.MD5OfBody);
                            }
                            if (!string.IsNullOrEmpty(message.Body))
                            {
                                Console.WriteLine("    Body: {0}", message.Body);
                                Console.WriteLine($"    {JsonConvert.DeserializeObject<Common.Message>(message.Body)}");
                            }

                            foreach (string attributeKey in message.Attributes.Keys)
                            {
                                Console.WriteLine("  Attribute");
                                Console.WriteLine("    Name: {0}", attributeKey);
                                var value = message.Attributes[attributeKey];
                                Console.WriteLine("    Value: {0}", string.IsNullOrEmpty(value) ? "(no value)" : value);
                            }
                            //Deleting a message
                            Console.WriteLine("Deleting the message.\n");
                            var deleteRequest =
                                new DeleteMessageRequest {QueueUrl = myQueueUrl, ReceiptHandle = message.ReceiptHandle};
                            sqs.DeleteMessage(deleteRequest);
                            totalReceived++;

                        }
                    }
                }
                //clean up queue
                Console.WriteLine("deleting queue");
                var deleteQueueResponse = sqs.DeleteQueue(myQueueUrl);
                Console.WriteLine($"deleting the queue resulted in {deleteQueueResponse.HttpStatusCode}");


            }
            catch (AmazonSQSException ex)
            {
                Console.WriteLine("Caught Exception: " + ex.Message);
                Console.WriteLine("Response Status Code: " + ex.StatusCode);
                Console.WriteLine("Error Code: " + ex.ErrorCode);
                Console.WriteLine("Error Type: " + ex.ErrorType);
                Console.WriteLine("Request ID: " + ex.RequestId);
            }

            Console.WriteLine("Press Enter to continue...");
            Console.Read();
        }
    }
}