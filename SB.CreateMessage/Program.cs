using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

namespace SB.CreateMessage
{
    class Program
    {
        const string ServiceBusConnectionString = "Endpoint=sb://filetrust-minikubedeployment.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=gZFOfH0CD+lAd6pp7DNTjFIcyLDXOD/K1cvspsck+BM=";
        const int noQueues = 11;
        static IQueueClient queueClient;

        static void Main(string[] args)
        {
                MainAsync().GetAwaiter().GetResult();
        }

        static async Task MainAsync()
        {
            const int numberOfMessages = 10;
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
                QueueName = queuename;

                queueClient = new QueueClient(ServiceBusConnectionString, QueueName);

                // Send messages.
                await SendMessagesAsync(numberOfMessages);

                Console.ReadKey();

                await queueClient.CloseAsync();
            }            
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
    }
}
