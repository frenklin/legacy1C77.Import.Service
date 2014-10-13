using System;
using System.Linq;
using Legacy1C77.Import.Service.DAL;
using System.Collections.Generic;
using System.Data;

namespace Legacy1C77.Import.Service.Core
{
    /// <summary>
    /// Thread from each imported 1Cv77 DB
    /// </summary>
    public class ImportDataFrom1Cv77 : BaseImportFrom1Cv77
    {

        public ImportDataFrom1Cv77(comClient1Cv77Adatpor com1Cv77Adaptor, sqlAdaptor sqlAdaptor):base(com1Cv77Adaptor, sqlAdaptor)
        {
            
        }

        public override bool Sync()
        {
            try
            {
                foreach (var tableName in OneCv77CatalogColumns.Select(s => s.tableName).Distinct())
                {
                    _sqlAdaptor.recreateTable(tableName, SqlColumns.Where(w => w.tableName.Equals(tableName)).ToList());
                    int page = 0;
                    var tableColumns = _comClient1Cv77Adatpor.getCatalogTableColumns(tableName);
                    while(true)
                    {
                        DataTable dt = _comClient1Cv77Adatpor.getCatalogDataTable(tableName, tableColumns, page);
                        _sqlAdaptor.bulkInsertData(tableName, dt);
                        if(dt.Rows.Count==0)
                        {
                            break;
                        }
                        page++;
                    }
                    
                    _sqlAdaptor.dropTable(string.Format("{0}_ok", tableName));
                    _sqlAdaptor.renameTable(tableName, string.Format("{0}_ok", tableName));
                }

                string tableNameData = string.Empty;
                foreach (var tableName in OneCv77DocumentsHeaderColumns.Select(s => s.tableName).Distinct())
                {
                    _sqlAdaptor.recreateTable(tableName, SqlColumns.Where(w => w.tableName.Equals(tableName)).ToList());

                    DateTime date = new DateTime(2003, 01, 01);
                    IEnumerable<TableColumn> dataDocTableExist = OneCv77DocumentsColumns.Where(
                                    s =>
                                    comClient1Cv77Adatpor.cleanSuffix(s.tableName) ==
                                    comClient1Cv77Adatpor.cleanSuffix(tableName));
                    if(dataDocTableExist.Count() > 0)
                    {
                        tableNameData = dataDocTableExist.First().tableName;
                        _sqlAdaptor.recreateTable(tableNameData, SqlColumns.Where(w => w.tableName.Equals(tableNameData)).ToList());
                    }

                    while(true)
                    {
                        List<string> dataRows = _comClient1Cv77Adatpor.GetDocumentData(tableName, date);

                        if (dataRows.Count > 0)
                        {
                            _sqlAdaptor.bulkInsertData(tableName, _comClient1Cv77Adatpor.getDocumentDataHeader(tableName, dataRows));
                            
                            if (dataDocTableExist.Count() > 0)
                            {
                                var dataTable = _comClient1Cv77Adatpor.getDocumentDataTable(tableNameData, dataRows);
                                if (dataTable.Rows.Count > 0)
                                {
                                    _sqlAdaptor.bulkInsertData(tableNameData, dataTable);
                                }
                            }
                        }

                        if (date > DateTime.Now)
                        {
                            break;
                        }

                        date = date.AddDays(1);
                    }

                    _sqlAdaptor.dropTable(string.Format("{0}_ok", tableName));
                    _sqlAdaptor.renameTable(tableName, string.Format("{0}_ok", tableName));

                     if(dataDocTableExist.Count() > 0)
                     {
                         _sqlAdaptor.dropTable(string.Format("{0}_ok", tableNameData));
                         _sqlAdaptor.renameTable(tableNameData, string.Format("{0}_ok", tableNameData));
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
    }
}
