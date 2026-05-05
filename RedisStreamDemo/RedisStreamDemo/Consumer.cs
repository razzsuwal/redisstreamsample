using StackExchange.Redis;

namespace RedisStreamDemo
{
    public class Consumer
    {
        private readonly IDatabase _db;
        private readonly string _streamName;
        private readonly string _groupName;
        private readonly string _consumerName;

        public Consumer(IDatabase db, string streamName, string groupName)
        {
            _db = db;
            _streamName = streamName;
            _groupName = groupName;
            _consumerName = $"consumer-{Guid.NewGuid().ToString()[..8]}";
        }

        public async Task InitializeAsync()
        {
            bool keyExists = await _db.KeyExistsAsync(_streamName);
            bool groupExists = keyExists &&
                (await _db.StreamGroupInfoAsync(_streamName))
                .Any(x => x.Name == _groupName);

            if (!groupExists)
            {
                await _db.StreamCreateConsumerGroupAsync(
                    _streamName,
                    _groupName,
                    "0-0",
                    true
                );
                Console.WriteLine($"[Consumer] Created group '{_groupName}' on stream '{_streamName}'");
            }
            else
            {
                Console.WriteLine($"[Consumer] Group '{_groupName}' already exists on stream '{_streamName}'");
            }
        }

        public async Task ConsumeAsync(CancellationToken token)
        {
            Console.WriteLine($"[Consumer] '{_consumerName}' listening on stream: '{_streamName}', group: '{_groupName}'");

            while (!token.IsCancellationRequested)
            {
                try
                {
                    // ── 1. Read new messages 
                    var messages = await _db.StreamReadGroupAsync(
                        _streamName,
                        _groupName,
                        _consumerName,
                        ">",
                        count: 10
                    );

                    if (messages.Length == 0)
                    {
                        await Task.Delay(500, token);
                        continue;
                    }

                    foreach (var message in messages)
                    {
                        try
                        {
                            // ── 2. Process the message 
                            Console.WriteLine($"\n[Consumer] ────────────────────────────");
                            Console.WriteLine($"[Consumer] Stream : {_streamName}");
                            Console.WriteLine($"[Consumer] ID     : {message.Id}");

                            foreach (var field in message.Values)
                            {
                                Console.WriteLine($"[Consumer] {field.Name,-15}: {field.Value}");
                            }

                            // ── 3. Acknowledge (mark as processed) ────
                            await _db.StreamAcknowledgeAsync(
                                _streamName,
                                _groupName,
                                message.Id
                            );
                            Console.WriteLine($"[Consumer] ✅ Acknowledged : {message.Id}");

                            // ── 4. Delete from stream (clear cache) ───
                            await _db.StreamDeleteAsync(
                                _streamName,
                                new RedisValue[] { message.Id }
                            );
                            Console.WriteLine($"[Consumer] 🗑️  Deleted      : {message.Id}");
                        }
                        catch (Exception ex)
                        {
                            // message failed - do NOT delete, retry later
                            Console.WriteLine($"[Consumer] ❌ Failed to process {message.Id}: {ex.Message}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine($"[Consumer] '{_consumerName}' stopped.");
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Consumer] Error: {ex.Message}");
                    await Task.Delay(2000, token);
                }
            }
        }
    }
}