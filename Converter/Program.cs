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
using System.Net;
using System.Threading;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft;
using Newtonsoft.Json;
using Common;



namespace SQSSample1
{

    class Program
    {
        public static void Main(string[] args)
        {
            var sqs = new AmazonSQSClient();
            var rnd = new Random();

            try
            {
                Console.WriteLine("===========================================");
                Console.WriteLine("Converter");
                Console.WriteLine("===========================================\n");

                //Receiving a message
                var receiveMessageRequest = new ReceiveMessageRequest
                {
                    QueueUrl = "https://queue.amazonaws.com/257724145439/NewDWFxFile.fifo",
                    MaxNumberOfMessages = 1,
                    WaitTimeSeconds = 20,
                    MessageAttributeNames = new List<string>() {"All"},
                    AttributeNames = new List<string>() {"All"}
                };
                while (true)
                {
                    var receiveMessageResponse = sqs.ReceiveMessage(receiveMessageRequest);
                    if (receiveMessageResponse.Messages != null)
                    {
                        Console.WriteLine("Printing received message.\n");
                        foreach (var message in receiveMessageResponse.Messages)
                        {
                            var recMsg = JsonConvert.DeserializeObject<Common.Message>(message.Body);
                            Console.WriteLine($"Processing {recMsg.S3Path}");
                            Thread.Sleep(TimeSpan.FromSeconds(rnd.Next(15)));
                            Console.WriteLine("Finished");
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
                                Console.WriteLine($"    {recMsg}");
                            }

                            foreach (string attributeKey in message.Attributes.Keys)
                            {
                                Console.WriteLine("  Attribute");
                                Console.WriteLine("    Name: {0}", attributeKey);
                                var value = message.Attributes[attributeKey];
                                Console.WriteLine("    Value: {0}", string.IsNullOrEmpty(value) ? "(no value)" : value);
                            }
                            //send ack
                            recMsg.Result = "OK";
                            recMsg.TypeOfMessage = MessageType.ProcessResponse;
                            recMsg.S3Path = "s3://us-east-1/path/to/someting.scz";
                            
                            var sendMessageRequest = new SendMessageRequest
                            {
                                QueueUrl = recMsg.CallbackQueue.ToString(), //callback queue
                                MessageBody = JsonConvert.SerializeObject(recMsg, Formatting.None)
                            };
                            var resp = sqs.SendMessage(sendMessageRequest);
                            if (resp.HttpStatusCode == HttpStatusCode.OK)
                            {
                                //Deleting a message
                                Console.WriteLine("Deleting the message.\n");
                                var deleteRequest =
                                    new DeleteMessageRequest
                                    {
                                        QueueUrl = "https://queue.amazonaws.com/257724145439/NewDWFxFile.fifo",
                                        ReceiptHandle = message.ReceiptHandle
                                    };
                                sqs.DeleteMessage(deleteRequest);

                            }


                        }

                    }

                }
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