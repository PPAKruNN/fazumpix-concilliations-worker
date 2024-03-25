using System.Text;
using System.Text.Json;
using ConcilliationWorker.Database;
using ConcilliationWorker.DTOs;
using ConcilliationWorker.Utils;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// Database.
const string connectionString = "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=fazumpix;Pooling=true";
await using var dataSource = NpgsqlDataSource.Create(connectionString);

const int chunkSize = 200_000;

// RabbitMQ.
ConnectionFactory factory = new()
{
  HostName = "localhost",
  UserName = "rabbit",
  Password = "mq"
};
IConnection connection = factory.CreateConnection();
IModel channel = connection.CreateModel();
channel.QueueDeclare(
    queue: "concilliations",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

EventingBasicConsumer consumer = new(channel);
consumer.Received += async (model, ea) =>
{
  // Set data for this concilliation.
  string concilliationId = Guid.NewGuid().ToString();
  await using var conn = await dataSource.OpenConnectionAsync();

  FileHelper fileHelper = new(concilliationId);

  Console.WriteLine($"[{concilliationId}] Started processing!");
  try
  {
    string serialized = Encoding.UTF8.GetString(ea.Body.ToArray());
    ProcessConcilliationDTO? dto = JsonSerializer.Deserialize<ProcessConcilliationDTO>(serialized);
    if (dto is null) throw new Exception("Invalid DTO");


    // Sorting. (fileToDatabase, statusDiff, ok).
    using (StreamReader pspFileReader = new StreamReader(dto.File, Encoding.UTF8))
    {
      var startTime = DateTime.Now;
      while (pspFileReader.Peek() > -1)
      {
        // Get chunks.
        List<ConcilliationDTO> pspPayments = await FileStreamReader.StreamReadPaymentChunk(pspFileReader, chunkSize);
        List<ConcilliationDTO> databasePayments = await DatabaseHandler.FindPayments(conn, pspPayments, dto.PaymentProviderId, dto.Date);

        await PaymentUtils.SortDiffAndMissing(pspPayments, databasePayments, fileHelper);

      }
      Console.WriteLine($"[{concilliationId}] Sorting finished: FileToDatabase/DifferentStatus! ({(DateTime.Now - startTime).TotalSeconds}sec)");
    }

    // Sorting. (databaseToFile)
    using (StreamWriter dbOnly = new(fileHelper.DatabaseToFileFilename, append: true))
    {
      var startTime = DateTime.Now;
      List<ConcilliationDTO> dbPayments = new();
      int iteration = 0;

      // Iterate over database.
      do
      {
        dbPayments = await DatabaseHandler.QueryPayments(conn, chunkSize, iteration, dto.PaymentProviderId, dto.Date);
        iteration++;

        await PaymentUtils.SortDatabaseOnly(dbPayments, fileHelper);

      }
      while (dbPayments.Count > 0);

      Console.WriteLine($"[{concilliationId}] Sorting finished: DatabaseToFile! ({(DateTime.Now - startTime).TotalSeconds}sec)");
    }

    // Assembly.
    var startTimeAssemble = DateTime.Now;
    await PaymentUtils.Assemble(fileHelper);
    Console.WriteLine($"[{concilliationId}] Assemble Finished! ({(DateTime.Now - startTimeAssemble).TotalSeconds}sec)");


    // Envie para a PSP novamente.
    StreamReader outputFile = new(fileHelper.AssemblyFilename);
    HttpContent content = new StreamContent(outputFile.BaseStream);

    HttpClient client = new();
    var response = client.PostAsync(dto.Postback, content);

    Console.WriteLine("Request finished!");
  }

  catch (Exception e)
  {
    Console.WriteLine(e.Message);
    Console.WriteLine(e.StackTrace);
    channel.BasicReject(ea.DeliveryTag, requeue: false);

    return;
  }

  // Cleaning.
  channel.BasicAck(ea.DeliveryTag, multiple: false);
  await conn.CloseAsync();
  fileHelper.CleanTempFiles();

  Console.WriteLine($"[{concilliationId}] Finished processing!");
};


channel.BasicConsume(
    queue: "concilliations",
    autoAck: false,
    consumer: consumer
);

Console.WriteLine("[*] Consumer is Ready!!!");
Console.WriteLine("Press [enter] to exit");
Console.ReadLine();

