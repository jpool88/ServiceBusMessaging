﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

namespace SB.SendAndReceive
{
    class Program
    {
        const string ServiceBusConnectionString = "Endpoint=sb://filetrust-minikubedeployment.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=gZFOfH0CD+lAd6pp7DNTjFIcyLDXOD/K1cvspsck+BM=";
        const int noQueues = 11;
        static IQueueClient queueClient;

        static void Main(string[] args)
        {
            SendAsync().GetAwaiter().GetResult();
            ReceiveAsync().GetAwaiter().GetResult();
        }

        static async Task SendAsync()
        {
            
            string QueueName;
            List<string> queuenames = new List<string>();
            queuenames.Add("MessageInspectionQueue");
            queuenames.Add("FileRouterQueue");
            queuenames.Add("Pre-AnalysisQueue");
            queuenames.Add("Post-AnalysisQueue");
            queuenames.Add("Pre-ProtectQueue");
            queuenames.Add("Post-ProtectQueue");
            queuenames.Add("ThreatCensorQueue");
            queuenames.Add("HeldFileRouterQueue");
            queuenames.Add("FilePreviewQueue");
            queuenames.Add("HeldReportQueue");
            queuenames.Add("MessageRegenerationQueue");
            queuenames.Add("SMTPTransmissionQueue");

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after sending all the messages.");
            Console.WriteLine("======================================================");

            foreach (string queuename in queuenames)
            {
                int numberOfMessages = new Random().Next(5, 100);
                QueueName = queuename;

                queueClient = new QueueClient(ServiceBusConnectionString, QueueName);

                // Send messages.
                await SendMessagesAsync(numberOfMessages);

                await queueClient.CloseAsync();
            }
            Console.ReadKey();
        }

        static async Task SendMessagesAsync(int numberOfMessagesToSend)
        {
            try
            {
                for (var i = 0; i < numberOfMessagesToSend; i++)
                {
                    // Create a new message to send to the queue.
                    string messageBody = $"Message {i}";
                    var message = new Message(Encoding.UTF8.GetBytes(messageBody));

                    // Write the body of the message to the console.
                    Console.WriteLine($"Sending message: {messageBody}");

                    // Send the message to the queue.
                    await queueClient.SendAsync(message);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }
        //-----------------------------------------------------
        //-----------------------------------------------------
        static async Task ReceiveAsync()
        {
            string QueueName;
            List<string> queuenames = new List<string>();
            queuenames.Add("MessageInspectionQueue");
            queuenames.Add("FileRouterQueue");
            queuenames.Add("Pre-AnalysisQueue");
            queuenames.Add("Post-AnalysisQueue");
            queuenames.Add("Pre-ProtectQueue");
            queuenames.Add("Post-ProtectQueue");
            queuenames.Add("ThreatCensorQueue");
            queuenames.Add("HeldFileRouterQueue");
            queuenames.Add("FilePreviewQueue");
            queuenames.Add("HeldReportQueue");
            queuenames.Add("MessageRegenerationQueue");
            queuenames.Add("SMTPTransmissionQueue");

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after receiving all the messages.");
            Console.WriteLine("======================================================");

            foreach (string queuename in queuenames)
            {
                QueueName = queuename;

                queueClient = new QueueClient(ServiceBusConnectionString, QueueName);

                // Register QueueClient's MessageHandler and receive messages in a loop
                RegisterOnMessageHandlerAndReceiveMessages();

                Console.ReadKey();

                await queueClient.CloseAsync();
            }
        }

        static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure the MessageHandler Options in terms of exception handling, number of concurrent messages to deliver etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of Concurrent calls to the callback `ProcessMessagesAsync`, set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
                // False below indicates the Complete will be handled by the User Callback as in `ProcessMessagesAsync` below.
                AutoComplete = false
            };

            // Register the function that will process messages
            queueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");

            // Complete the message so that it is not received again.
            // This can be done only if the queueClient is created in ReceiveMode.PeekLock mode (which is default).
            await queueClient.CompleteAsync(message.SystemProperties.LockToken);

            // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
            // If queueClient has already been Closed, you may chose to not call CompleteAsync() or AbandonAsync() etc. calls 
            // to avoid unnecessary exceptions.
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
    }
}
