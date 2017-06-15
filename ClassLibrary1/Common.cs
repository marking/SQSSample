using System;

namespace Common
{
    public enum MessageType
    {
        ProcessRequest,
        ProcessResponse
    }
    public class Message
    {
        public MessageType TypeOfMessage { get; set; }
        public string S3Path { get; set; }
        public string Result { get; set; }
        public Uri CallbackQueue { get; set; }
        public int TotalMessages { get; set; }
        public int ThisMessage { get; set; }
        

        public override string ToString()
        {
            return $"type: {TypeOfMessage}, s3: {S3Path}, result: {Result}, callback: {CallbackQueue}";
        }
    }
}
