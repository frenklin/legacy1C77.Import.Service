using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Legacy1C77.Import.Service.DAL;

namespace Legacy1C77.Import.Service.Core
{
    public class ParticularImportDataFrom1Cv77:BaseImportFrom1Cv77
    {
        public ParticularImportDataFrom1Cv77(comClient1Cv77Adatpor com1Cv77Adaptor, sqlAdaptor sqlAdaptor):base(com1Cv77Adaptor, sqlAdaptor)
        {
            
        }

        public override bool Sync()
        {
            try
            {
                DateTime dateFrom = DateTime.Now.AddDays(-Config.SyncDatabases.lastDays.Value);
                IEnumerable<TableColumn> dataDocTableExist = null;
                string tableNameData = string.Empty;

                foreach (var tableName in OneCv77CatalogColumns.Select(s => s.tableName).Distinct())
                {
                    _sqlAdaptor.recreateTable(tableName, OneCv77CatalogColumns.Where(w => w.tableName.Equals(tableName)).ToList());
                    int page = 0;
                    var tableColumns = _comClient1Cv77Adatpor.getCatalogTableColumns(tableName);
                    while (true)
                    {
                        DataTable dt = _comClient1Cv77Adatpor.getCatalogDataTable(tableName, tableColumns, page);
                        _sqlAdaptor.bulkInsertData(tableName, dt);
                        if (dt.Rows.Count == 0)
                        {
                            break;
                        }
                        page++;
                    }

                    _sqlAdaptor.dropTable(string.Format("{0}_ok", tableName));
                    _sqlAdaptor.renameTable(tableName, string.Format("{0}_ok", tableName));
                }

                foreach (var tableName in OneCv77DocumentsHeaderColumns.Select(s => s.tableName).Distinct())
                {
                    DateTime date = dateFrom;

                    dataDocTableExist = OneCv77DocumentsColumns.Where(
                           s =>
                           comClient1Cv77Adatpor.cleanSuffix(s.tableName) ==
                           comClient1Cv77Adatpor.cleanSuffix(tableName));

                    if (dataDocTableExist.Count() > 0)
                    {
                        tableNameData = dataDocTableExist.First().tableName;
                        if (!_sqlAdaptor.deleteRefTable(tableName+"_ok", tableNameData + "_ok", dateFrom))
                        {
                            Logger.logger.Error(string.Format("Can't particular sync {0}", tableNameData));
                            tableNameData = string.Empty;
                        }
                    }else
                    {
                        tableNameData = string.Empty;
                    }

                    if (_sqlAdaptor.deleteTable(tableName + "_ok", dateFrom))
                    {
                        while (true)
                        {
                            List<string> dataRows = _comClient1Cv77Adatpor.GetDocumentData(tableName, date);

                            if (dataRows.Count > 0)
                            {
                                _sqlAdaptor.bulkInsertData(tableName + "_ok",
                                                       _comClient1Cv77Adatpor.getDocumentDataHeader(tableName, dataRows));

                                if (dataDocTableExist.Count() > 0 && tableNameData != string.Empty)
                                {
                                    var dataTable = _comClient1Cv77Adatpor.getDocumentDataTable(tableNameData, dataRows);
                                    if (dataTable.Rows.Count > 0)
                                    {
                                        _sqlAdaptor.bulkInsertData(tableNameData + "_ok", dataTable);
                                    }
                                }
                            }

                            if (date > DateTime.Now.AddDays(10))
                            {
                                break;
                            }

                            date = date.AddDays(1);
                        }
                    }
                    else
                    {
                        Logger.logger.Error(string.Format("Can't particular sync {0}", tableName));
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
