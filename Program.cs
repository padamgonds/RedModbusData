using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using FluentModbus;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Alto.ConsumersBase.Model;

class Program
{
    static Timer timer;
    static IServiceCollection services;
    static IServiceProvider serviceProvider;
    static async Task Main(string[] args)
    {
        int intervalInSeconds = GetIntervalFromConfiguration();
        StartTimer(intervalInSeconds);

        Console.WriteLine("Press Enter to stop the execution...");
        Console.ReadLine();

        StopExecution();
    }

    static int GetIntervalFromConfiguration()
    {
        return int.Parse(ConfigurationManager.AppSettings["IntervalInSeconds"]);
    }

    static void StartTimer(int intervalInSeconds)
    {
        timer = new Timer(async (_) => await ExecuteModbusToEventHubAsync(), null, TimeSpan.Zero, TimeSpan.FromSeconds(intervalInSeconds));
    }

    static async Task StopExecution()
    {
        timer.Dispose();
        await Task.Delay(Timeout.Infinite);
    }

    static async Task ExecuteModbusToEventHubAsync()
    {
        var blobConnectionString = ConfigurationManager.AppSettings["BlobConnectionString"];
        var blobContainerName = ConfigurationManager.AppSettings["BlobContainerName"];
        var eventHubConnectionString = ConfigurationManager.AppSettings["EventHubConnectionString"];
        var eventHubName = ConfigurationManager.AppSettings["EventHubName"];

        var blobServiceClient = new BlobServiceClient(blobConnectionString);
        var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);

        var logFileName = $"modbus_log_{DateTime.UtcNow:yyyyMMdd_HHmmss}.txt";
        var blobClient = blobContainerClient.GetBlobClient(logFileName);

        using (var logFileStream = new MemoryStream())
        {
            var startTime = DateTime.Now;
            WriteLogStartTime(logFileStream, startTime);

            var ipAddress = ConfigurationManager.AppSettings["ModbusIpAddress"];
            var client = new ModbusTcpClient();

            try
            {
                client.ConnectTimeout = 1000;
                var endpoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse(ipAddress), 502);
                client.Connect(endpoint, ModbusEndianness.BigEndian);
                var address = 2000;
                var i = 42001;

                var modbusData = await client.ReadHoldingRegistersAsync<float>(1, address, 36);

                await using (var eventHubProducerClient = new EventHubProducerClient(eventHubConnectionString, eventHubName))
                {
                    var tagMappings = GetTagMappings();

                    foreach (var register in modbusData.ToArray())
                    {
                        if (tagMappings.TryGetValue(i, out var mapping))
                        {
                            var tag = mapping.Tag;
                            var metricKeys = mapping.MetricKeys;

                            var metrics = new Dictionary<string, string>();

                            for (int j = 0; j < metricKeys.Count; j++)
                            {
                                var metricKey = metricKeys[j];
                                if (!string.IsNullOrEmpty(metricKey))
                                {
                                    metrics[metricKey] = register.ToString();
                                }
                            }

                            var universalEventHubMessage = new UniversalEventHubMessage
                            {
                                Tag = tag,
                                LocationCode = "DAMMA-ARMST-STRM1",
                                TimeStamp = DateTime.UtcNow,
                                Metrics = metrics
                            };

                            var envelope = new Envelope
                            {
                                messageType = new string[]
                                {
                                        "urn:message:Alto.ConsumersBase.Model:UniversalEventHubMessage"
                                },
                                message = universalEventHubMessage
                            };

                            var jsonMessage = JsonSerializer.Serialize(envelope);
                            var eventData = new EventData(Encoding.UTF8.GetBytes(jsonMessage));
                            var logMessage = $"{i}: {register}\n";

                            await SendEventData(eventHubProducerClient, eventData);
                            Console.WriteLine(logMessage);
                        }

                        i++;
                        i++;
                    }

                }

                Console.WriteLine("Modbus data retrieval and Event Hub sending completed successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occurred: {ex.Message}");
                WriteErrorMessage(logFileStream, ex.Message);
            }
            finally
            {
                client.Disconnect();
            }

            var endTime = DateTime.Now;
            WriteLogEndTime(logFileStream, endTime);
            await UploadLogFile(blobClient, logFileStream);
        }
    }

            static void WriteLogStartTime(MemoryStream logFileStream, DateTime startTime)
            {
                var logMessage = $"Log Start Time: {startTime}\n";
                var logMessageBytes = Encoding.UTF8.GetBytes(logMessage);
                logFileStream.Write(logMessageBytes, 0, logMessageBytes.Length);
            }

            static void WriteLogEndTime(MemoryStream logFileStream, DateTime endTime)
            {
                var logMessage = $"Log End Time: {endTime}\n";
                var logMessageBytes = Encoding.UTF8.GetBytes(logMessage);
                logFileStream.Write(logMessageBytes, 0, logMessageBytes.Length);
            }

            static void WriteErrorMessage(MemoryStream logFileStream, string errorMessage)
            {
                var logMessage = $"Error occurred: {errorMessage}\n";
                var logMessageBytes = Encoding.UTF8.GetBytes(logMessage);
                logFileStream.Write(logMessageBytes, 0, logMessageBytes.Length);
            }

            static Dictionary<int, (string Tag, List<string> MetricKeys)> GetTagMappings()
            {
                        return new Dictionary<int, (string Tag, List<string> MetricKeys)>
                    {
                        { 42001, ("FIT-90000", new List<string> { "Gross_Volume_Flow_Rate", "" }) },
                        { 42003, ("GC-1", new List<string> {  "Standard_Flowrate", ""  }) },
                        { 42005, ("FIT-90000", new List<string> {  "Mass_Flow_Rate", ""  }) },
                        { 42007, ("FIT-90000", new List<string> {  "Energy_Flow_Rate", ""  }) },
                        { 42009, ("TIT-90000", new List<string> {  "Independent_Temperature", ""  }) },
                        { 42011, ("PIT-90000", new List<string> {  "Independent_Pressure", "" }) },
                        { 42013, ("FC-90000", new List<string>  { "Stream1_GIV", "" }) },
                        { 42015, ("FC-90000", new List<string>  { "Stream1_GSV", "" }) },
                        { 42017, ("FC-90000", new List<string>  { "Stream1_MAS", "" }) },
                        { 42019, ("FC-90000", new List<string>  { "Stream1_EGY", "" }) },
                        { 42021, ("FC-90000", new List<string>  { "Station1_GIV", "" }) },
                        { 42023, ("FC-90000", new List<string>  { "Station1_GSV", "" }) },
                        { 42025, ("FC-90000", new List<string>  { "Station1_MAS", "" }) },
                        { 42027, ("FC-90000", new List<string>  { "Station1_EGY", "" }) },
                        { 42029, ("GC-1", new List<string>  { "Comp_C1", "" }) },
                        { 42031, ("GC-1", new List<string>  { "Comp_N2", "" }) },
                        { 42033, ("GC-1", new List<string>  { "Comp_CO2", ""}) },
                        { 42035, ("GC-1", new List<string>  { "Comp_C2", "" }) },
                        { 42037, ("GC-1", new List<string>  { "Comp_C3", "" }) },
                        { 42039, ("GC-1", new List<string>  { "Comp_H2O", "" }) },
                        { 42041, ("GC-1", new List<string>  { "Comp_H2S", "" }) },
                        { 42043, ("GC-1", new List<string>  { "Comp_H2", ""  }) },
                        { 42045, ("GC-1", new List<string>  { "Comp_C1O", "" }) },
                        { 42047, ("GC-1", new List<string>  { "Comp_O2", ""  }) },
                        { 42049, ("GC-1", new List<string>  { "Comp_IC4", "" }) },
                        { 42051, ("GC-1", new List<string>  { "Comp_NC4", "" }) },
                        { 42053, ("GC-1", new List<string>  { "Comp_IC5", "" }) },
                        { 42055, ("GC-1", new List<string>  { "Comp_NC5", "" }) },
                        { 42057, ("GC-1", new List<string>  { "Comp_NC6", "" }) },
                        { 42059, ("GC-1", new List<string>  { "Comp_NC7", "" }) },
                        { 42061, ("GC-1", new List<string>  { "Comp_NC8", "" }) },
                        { 42063, ("GC-1", new List<string>  { "Comp_NC9", "" }) },
                        { 42065, ("GC-1", new List<string>  { "Comp_NC10", "" }) },
                        { 42067, ("GC-1", new List<string>  { "Comp_HE", "" }) },
                        { 42069, ("GC-1", new List<string>  { "Comp_AR", "" }) },
                        { 42071, ("GC-1", new List<string>  { "Comp_NEOC5", "" }) }
        };
    }

    static async Task SendEventData(EventHubProducerClient eventHubProducerClient, EventData eventData)
    {
        await eventHubProducerClient.SendAsync(new List<EventData> { eventData });
    }

    static async Task UploadLogFile(BlobClient blobClient, MemoryStream logFileStream)
    {
        logFileStream.Position = 0;
        await blobClient.UploadAsync(logFileStream, true);
    }
}
