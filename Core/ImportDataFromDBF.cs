using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Legacy1C77.Import.Service.DAL;
using System.Data;

namespace Legacy1C77.Import.Service.Core
{
    public class ImportDataFromDBF
    {
        protected sqlAdaptor _sqlAdaptor = null;
        protected dbf1Cv77Adaptor _dbf1Cv77Adaptor = null;

        public ImportDataFromDBF(dbf1Cv77Adaptor dbf1Cv77Adaptor, sqlAdaptor sqlAdaptor)
        {
            this._sqlAdaptor = sqlAdaptor;
            this._dbf1Cv77Adaptor = dbf1Cv77Adaptor;
        }

        public bool Sync()
        {
            try
            {
                foreach (var tableName in _dbf1Cv77Adaptor.TablesDescribe.Select(s => s.tableName).Distinct())
                {
                    _sqlAdaptor.recreateTable(tableName, _dbf1Cv77Adaptor.TablesDescribe.Where(w => w.tableName.Equals(tableName)).ToList());


                    DataTable dt = _dbf1Cv77Adaptor.getDataTable(tableName);
                    _sqlAdaptor.bulkInsertData(tableName, dt);
                   

                    _sqlAdaptor.dropTable(string.Format("{0}_ok", tableName));
                    _sqlAdaptor.renameTable(tableName, string.Format("{0}_ok", tableName));
                    Logger.logger.Debug(string.Format("ImportDataFromDBF {0} synced",tableName));
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
