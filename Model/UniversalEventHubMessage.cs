using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Alto.ConsumersBase.Model
{
    public class UniversalEventHubMessage
    {
        public string Tag { get; set; }

        public string LocationCode { get; set; }

        public DateTime TimeStamp { get; set; }

        public Dictionary<string, string> Metrics { get; set; }

    }
}
