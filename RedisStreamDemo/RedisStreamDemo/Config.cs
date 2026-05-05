namespace RedisStreamDemo
{
    public class RedisConfig
    {
        public string ConnectionString { get; set; } = "localhost:6379";
        public List<StreamConfig> Streams { get; set; } = new();
    }

    public class StreamConfig
    {
        public string StreamName { get; set; } = string.Empty;
        public string GroupName { get; set; } = string.Empty;
    }
}
