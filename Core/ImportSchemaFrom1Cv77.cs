using System;
using System.Linq;
using Legacy1C77.Import.Service.DAL;

namespace Legacy1C77.Import.Service.Core
{
    public class ImportSchemaFrom1Cv77:BaseImportFrom1Cv77
    {
        
        public ImportSchemaFrom1Cv77(comClient1Cv77Adatpor com1Cv77Adaptor, sqlAdaptor sqlAdaptor):base(com1Cv77Adaptor, sqlAdaptor)
        {
            
        }

        public override bool Sync()
        {
            try
            {
                foreach (var tableName in OneCv77CatalogColumns.Select(s => s.tableName).Distinct())
                {
                    if (!equalCatalogTableDescribe(tableName))
                    {
                        _sqlAdaptor.recreateTable(tableName, OneCv77CatalogColumns.Where(t => t.tableName.Equals(tableName)).ToList());
                    }
                }
                foreach (var tableName in OneCv77DocumentsColumns.Select(s => s.tableName).Distinct())
                {
                    if (!equalDocumentTableDescribe(tableName))
                    {
                        _sqlAdaptor.recreateTable(tableName, OneCv77DocumentsColumns.Where(t => t.tableName.Equals(tableName)).ToList());
                    }
                }
                foreach (var tableName in OneCv77DocumentsHeaderColumns.Select(s => s.tableName).Distinct())
                {
                    if (!equalDocumentHeaderDescribe(tableName))
                    {
                        _sqlAdaptor.recreateTable(tableName, OneCv77DocumentsHeaderColumns.Where(t => t.tableName.Equals(tableName)).ToList());
                    }
                }
                
            }
            catch (Exception ex)
            {
                Logger.logger.Error(ex);
                return false;
            }
            return true;
        }

        protected bool equalCatalogTableDescribe(string tableName)
        {
            foreach (var oneCcol in OneCv77CatalogColumns.Where(c => c.tableName.Equals(tableName)))
            {
                if (!SqlColumns.Any(c => c.tableName.Equals(tableName) && c.columnName.Equals(oneCcol.columnName)))
                {
                    return false;
                }
            }
            return true;
        }

        protected bool equalDocumentTableDescribe(string tableName)
        {
            foreach (var oneCcol in OneCv77DocumentsColumns.Where(c => c.tableName.Equals(tableName)))
            {
                if (!SqlColumns.Any(c => c.tableName.Equals(tableName) && c.columnName.Equals(oneCcol.columnName)))
                {
                    return false;
                }
            }
            return true;
        }

        protected bool equalDocumentHeaderDescribe(string tableName)
        {
            foreach (var oneCcol in OneCv77DocumentsHeaderColumns.Where(c => c.tableName.Equals(tableName)))
            {
                if (!SqlColumns.Any(c => c.tableName.Equals(tableName) && c.columnName.Equals(oneCcol.columnName)))
                {
                    return false;
                }
            }
            return true;
        }
    }
}
