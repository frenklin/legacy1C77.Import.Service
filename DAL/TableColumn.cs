using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Legacy1C77.Import.Service.DAL
{
    public class TableColumn
    {
        public string tableNameSystem { get; set; }
        public string tableName { get; set; }
        public string columnName { get; set; }
        public string columnNameSystem { get; set; }
        public string dataType { get; set; }
        public int length { get; set; }
    }
}
