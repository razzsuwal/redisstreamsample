using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using RedisStreamDemo;

// ── 1. Load Configuration
var config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false)
    .AddEnvironmentVariables()
    .Build();

var redisConfig = config.GetSection("Redis").Get<RedisConfig>()
    ?? throw new Exception("Redis config missing");

// ── 2. Setup Redis Connection
var muxer = ConnectionMultiplexer.Connect(redisConfig.ConnectionString);
var db = muxer.GetDatabase();

var tokenSource = new CancellationTokenSource();
var token = tokenSource.Token;

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    tokenSource.Cancel();
    Console.WriteLine("Shutting down...");
};

// ── 3. Initialize Consumers 
var consumers = redisConfig.Streams
    .Select(s => new Consumer(db, s.StreamName, s.GroupName))
    .ToList();

foreach (var consumer in consumers)
{
    await consumer.InitializeAsync();
}

// ── 5. Initialize Producers 
var producers = redisConfig.Streams
    .Select(s => new Producer(db, s.StreamName))
    .ToList();

// ── 6. Run Producers and Consumers Concurrently 
var producerTasks = producers.Select(p => p.ProduceContinuouslyAsync(token));
var consumerTasks = consumers.Select(c => c.ConsumeAsync(token));

await Task.WhenAll(producerTasks.Concat(consumerTasks));