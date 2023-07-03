using MassTransit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Alto.ConsumersBase.Model
{
    public class Envelope
    {
        public string[] messageType { get; set; }
        public UniversalEventHubMessage message { get; set; }    
    }
}
