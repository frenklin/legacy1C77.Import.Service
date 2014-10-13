using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Runtime.InteropServices;
using Legacy1C77.Import.Service.Core;
using System.Text;

namespace Legacy1C77.Import.Service.DAL
{
    public class comClient1Cv77Adatpor:IDisposable
    {
        private object _v7 = null;
        private Type _v7Type = null;
        private string _userName = string.Empty;
        private string _password = string.Empty;
        private string _pathDB = string.Empty;
        private enConnectionState _State=enConnectionState.Closed;
        private readonly string OneCMainFunctionName = "ExportEnterprise";
        private string _dateFrom = "15/08/12";
        private string _dateTo = "01/01/13";
        private int pageSize = 100;
 
        //private int firstRecord = 1;
        //private int lastRecord = 10;
        // "01/10/13" 1C date format

      
        public enConnectionState State
        {
            get { return _State; }
        }

        public comClient1Cv77Adatpor(string userName, string password, string pathDB)
        {
            this._password = password;
            this._userName = userName;
            this._pathDB = pathDB;
            Initialize();
        }
      
       
        #region LowLevel Methods
        private void Initialize()
        {
            try
            {
                _v7 = Activator.CreateInstance(Type.GetTypeFromProgID("V77.Application", true));
                _v7Type = _v7.GetType();
                _State = enConnectionState.InstanceCreated;
            }
            catch (Exception e)
            {
                Logger.logger.Error("Can't initialize V77.Application" + e.Message);
                throw;
            }
            Logger.logger.Debug("Initialize V77.Application");
        }

        public bool Login()
        {
            bool result = false;
            if (_State == enConnectionState.InstanceCreated)
            {
                try
                {
                    var args = new Object[3];
                    args[0] = _v7Type.InvokeMember(@"RMTrade", BindingFlags.Public | BindingFlags.InvokeMethod, null,
                                                   _v7,
                                                   null);
                    args[1] = string.Format(@"/d""{0}"" /n{1} /p{2}", _pathDB, _userName, _password); //D:\\1S_Data
                    args[2] = "NO_SPLASH_SHOW";
                    result =
                        (Boolean)
                        _v7Type.InvokeMember(@"Initialize",
                                             BindingFlags.Public | BindingFlags.InvokeMethod | BindingFlags.Static, null,
                                             _v7, args);
                    if (result)
                    {
                        _State = enConnectionState.Connected;
                    }
                }
                catch (Exception e)
                {
                    Logger.logger.Error(string.Format("Can't login to DB: '{0}' Error: '{1}'", _pathDB, e.Message));
                    throw;
                }
            }
            return result;
        }

        public void Dispose()
        {
            if (_v7 != null)
            {
                Marshal.Release(Marshal.GetIDispatchForObject(_v7));
                Marshal.ReleaseComObject(_v7);
                _v7 = null;
                _v7Type = null;
            }
            _State = enConnectionState.Closed;
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        protected string[] GetDocumentDescribe(string name)
        {
            name = name.Trim();
            if (_State != enConnectionState.Connected)
            {
                var e = new Exception(string.Format("GetDocumentDescribe '{0}' Not connected to 1C DB:'{1}'", name, _pathDB));
                Logger.logger.Error(e);
                throw e;
            }
            string[] sRes;
            Logger.logger.Debug(string.Format("GetDocumentDescribe '{0}' DB:'{1}'", name, _pathDB));
            try
            {
                var result = _v7Type.InvokeMember(OneCMainFunctionName, BindingFlags.Public | BindingFlags.InvokeMethod, null, _v7, new object[] { "5", cleanSuffix(name), _dateFrom, _dateTo, "1", "1" });
                sRes = result.ToString().Split(Config.LINE_SEPARATOR, StringSplitOptions.RemoveEmptyEntries);
            }
            catch (Exception e)
            {
                Logger.logger.Error(string.Format("GetDocumentDescribe '{0}' DB:'{1}' Message:{2}", name, _pathDB, e.Message));
                throw;
            }
            return sRes;
        }

        protected string[] GetCatalogDescribe(string name)
        {
            name = name.Trim();
            if (_State != enConnectionState.Connected)
            {
                var e = new Exception(string.Format("GetCatalogDescribe '{0}' Not connected to 1C DB:'{1}'",name, _pathDB));
                Logger.logger.Error(e);
                throw e;
            }
            string[] sRes;
            Logger.logger.Debug(string.Format("GetCatalogDescribe '{0}' DB:'{1}'", name, _pathDB));
            try
            {
                var result = _v7Type.InvokeMember(OneCMainFunctionName, BindingFlags.Public | BindingFlags.InvokeMethod, null, _v7, new object[] { "3", name, _dateFrom, _dateTo, "1", "1" });
                sRes = result.ToString().Split(Config.LINE_SEPARATOR, StringSplitOptions.RemoveEmptyEntries);
            }
            catch (Exception e)
            {
                Logger.logger.Error(string.Format("GetCatalogDescribe '{0}' DB:'{1}' Message:{2}", name, _pathDB, e.Message));
                throw;
            }
            return sRes;
        }

        public static string cleanSuffix(string tableName)
        {
            if (tableName.LastIndexOf("Шапка") > 0 || tableName.LastIndexOf("Табл") > 0)
            {
                if (tableName.LastIndexOf("Шапка") + 5 == tableName.Length)
                {
                    tableName = tableName.Substring(0, tableName.Length - 5);
                }
                else if (tableName.LastIndexOf("Табл") + 4 == tableName.Length)
                {
                    tableName = tableName.Substring(0, tableName.Length - 4);
                }
            }
            return tableName;
        }

        public List<string> GetDocumentData(string tableName, DateTime date)
        {
            if (_State != enConnectionState.Connected)
            {
                var e = new Exception(string.Format("GetDocumentData '{0}' Not connected to 1C DB: '{1}'", tableName, _pathDB));
                Logger.logger.Error(e);
                throw e;
            }
            string result = string.Empty;
            Logger.logger.Debug(string.Format("Date: '{2}' GetDocumentData '{0}' " +
                                              "DB: '{1}'", tableName, _pathDB, date.ToString("dd/MM/yyyy")));
            try
            {
               
                
                //StringBuilder result = new StringBuilder();
              
                    result = _v7Type.InvokeMember(OneCMainFunctionName,
                                                      BindingFlags.Public | BindingFlags.InvokeMethod, null, _v7,
                                                      new object[]
                                                          {
                                                              "6", cleanSuffix(tableName), date.ToString("dd/MM/yyyy"), date.ToString("dd/MM/yyyy"),
                                                              "1", "250"
                                                          }).ToString();
                   
            }
            catch (Exception e)
            {
                Logger.logger.Error(string.Format("Date: '{3}' GetDocumentData '{0}' DB: '{1}' Message:{2}", date, tableName, _pathDB, e.Message));
                throw;
            }
            return new List<string>(result.ToString().Split(Config.LINE_SEPARATOR, StringSplitOptions.RemoveEmptyEntries));
        }
        
        protected string[] GetCatalogData(string tableName, int pageNum)
        {
            if (_State != enConnectionState.Connected)
            {
                var e = new Exception(string.Format("GetCatalogData '{0}' Not connected to 1C DB: '{1}'", tableName, _pathDB));
                Logger.logger.Error(e);
                throw e;
            }
            string[] sRes;
            Logger.logger.Debug(string.Format("GetCatalogData '{0}' DB: '{1}' Page:{2}", tableName, _pathDB, pageNum));
            try
            {
                //string result = string.Empty;
                string result = _v7Type.InvokeMember(OneCMainFunctionName, BindingFlags.Public | BindingFlags.InvokeMethod, null, _v7, new object[] { "2", tableName, _dateFrom, _dateTo, (pageNum * pageSize + 1).ToString(), (pageNum * pageSize + pageSize).ToString() }).ToString();    
                   
                sRes = result.ToString().Split(Config.LINE_SEPARATOR, StringSplitOptions.RemoveEmptyEntries);
            }
            catch (Exception e)
            {
                sRes = new string[]{};
                Logger.logger.Error(string.Format("GetCatalogData '{0}' DB: '{1}' Page:{3} Message:{2}", tableName, _pathDB, e.Message, pageNum));
                throw;
            }
            return sRes;
        }

        protected string[] GetDocumentList()
        {
            if (_State != enConnectionState.Connected)
            {
                var e = new Exception(string.Format("GetDocumentList Not connected to 1C DB: '{0}'", _pathDB));
                Logger.logger.Error(e);
                throw e;
            }
            string[] sRes;
            Logger.logger.Debug(string.Format("GetDocumentList DB: '{0}'", _pathDB));
         
            try
            {
                var result = _v7Type.InvokeMember(OneCMainFunctionName, BindingFlags.Public | BindingFlags.InvokeMethod, null, _v7, new object[] { "4", "", _dateFrom, _dateTo, "1", "1" });
                sRes = result.ToString().Split(Config.LINE_SEPARATOR, StringSplitOptions.RemoveEmptyEntries);
            }
            catch (Exception e)
            {
                Logger.logger.Error(string.Format("GetDocumentList DB: '{0}' Message:{1}", _pathDB, e.Message));
                throw;
            }
            return sRes;
        }

        protected string[] GetCatalogList()
        {
            if (_State != enConnectionState.Connected)
            {
                var e = new Exception(string.Format("GetCatalogList Not connected to 1C DB: '{0}'", _pathDB));
                Logger.logger.Error(e);
                throw e;
            }
            string[] sRes;
            Logger.logger.Debug(string.Format("GetCatalogList DB: '{0}'", _pathDB));
            try
            {
                var result = _v7Type.InvokeMember(OneCMainFunctionName, BindingFlags.Public | BindingFlags.InvokeMethod, null, _v7, new object[] { "1", "", _dateFrom, _dateTo, "1", "1" });
                sRes = result.ToString().Split(Config.LINE_SEPARATOR, StringSplitOptions.RemoveEmptyEntries);
            }
            catch (Exception e)
            {
                Logger.logger.Error(string.Format("GetCatalogList DB: '{0}' Message:{1}",_pathDB, e.Message));
                throw;
            }
            
            return sRes;
        }

        #endregion

     
        public Dictionary<string, string> getCatalogTableColumns(string tableName)
        {
            Dictionary<string, string> result=new Dictionary<string, string>();
            string[] cols=GetCatalogDescribe(tableName);
            var columns=cols[0].Split(Config.COL_SEPARATOR, StringSplitOptions.None);
            bool skipFirst = false;
            foreach (string column in columns)
            {
                if(!skipFirst)
                {
                    skipFirst = true;
                }else
                {
                    if (column.Trim() != string.Empty)
                    {
                        result.Add(column.Trim(), "nvarchar(512)");
                    }
                }
            }
            return result;
        }

        #region GetDocColumns
        public Dictionary<string, string> getDocumentHeaderColumns(string tableName)
        {
            var result = new Dictionary<string, string>();
            string[] cols = GetDocumentDescribe(tableName);
            var headerTable = cols[0].Split(Config.TABLE_SEPARATOR, StringSplitOptions.None);
            if (headerTable.Length == 2)
            {
                result.Add("hashKey", "nvarchar(256)");
                result.Add("НомерДок", "nvarchar(256)");
                if (tableName=="ОперацияШапка")
                {
                   result.Add("Документ", "nvarchar(256)");
                }
                result.Add("ДатаДок", "nvarchar(256)");
                result.Add("НомерСтроки", "nvarchar(256)");
                parseDocFields(headerTable[0], ref result);
            }

            return result;
        }


        public Dictionary<string, string> getDocumentTableColumns(string tableName)
        {
            var result = new Dictionary<string, string>();
            string[] cols = GetDocumentDescribe(tableName);
            var docTable = cols[0].Split(Config.TABLE_SEPARATOR, StringSplitOptions.None);
            if (docTable.Length == 2 && !string.IsNullOrWhiteSpace(docTable[1]))
            {
                result.Add("hashKey", "nvarchar(512)");
                return parseDocFields(docTable[1], ref result);
            }

            return result;
        }


        private Dictionary<string, string> parseDocFields(string metaInfo, ref Dictionary<string, string> result)
        {
            
            var columns = metaInfo.Split(Config.COL_SEPARATOR, StringSplitOptions.None);

            foreach (string column in columns)
            {
                if (!string.IsNullOrWhiteSpace(column))
                {
                    if (column.Length > 3 && column.Substring(0, 3) == "Тип")
                    {
                        var colInfo = column.Split(Config.COL_INFO, StringSplitOptions.None);
                        string colName = string.Empty;
                        string colType = string.Empty;
                        foreach (string info in colInfo)
                        {
                            var s = info.Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
                            if (info.Substring(0, 3).Contains("инд") && s.Length == 2)
                            {
                                colName = s[1]; // :) @инд Счет
                            }
                            if (info.Substring(0, 3).Contains("Тип") && s.Length == 2)
                            // TODO Convert Types from 1C to MSSQL
                            {
                                colType = s[1]; // :) @Тип Число
                            }

                        }
                        if (colName != string.Empty && colType != string.Empty)
                        {
                            result.Add(colName, "nvarchar(512)"); // TODO Convert Types from 1C to MSSQL
                        }
                        else
                        {
                            Logger.logger.Debug(
                                string.Format(
                                    "getDocumentTableColumns DB: '{0}' Message: Unparsed column info {1}", _pathDB,
                                    column));
                        }
                    }
                }
            }
            return result;
        }
        #endregion

        public DataTable getCatalogDataTable(string tableName, Dictionary<string, string> tableColumns, int pageNum)
        {
            DataTable result=new DataTable(tableName);
            string[] dataRows = GetCatalogData(tableName, pageNum);
            
            foreach (var columnName in tableColumns)
            {
                result.Columns.Add(new DataColumn(columnName.Key, typeof (string)));
            }
            foreach (string dataRow in dataRows)
            {
                string[] dataValues = dataRow.Split(Config.COL_SEPARATOR, StringSplitOptions.None);
                if(dataValues.Length==tableColumns.Count+1)
                {
                    DataRow row = result.NewRow();
                    for (int i = 0; i < tableColumns.Count; i++)
                    {
                        if (dataValues[i].Length > 512)
                        {
                            row[i] = dataValues[i].Substring(0, 510).Trim();
                            Logger.logger.Warn("getCatalogDataTable truncate to 512 chars string table:{0} page:{1}", tableColumns, pageNum);
                        }
                        else
                        {
                            row[i] = dataValues[i].Trim();
                        }
                    }
                    result.Rows.Add(row);
                }else
                {
                    Logger.logger.Error(string.Format(
                           "getCatalogDataTable DB: '{0}' Message: Wrong cols number {1}", _pathDB,
                           tableName));
                }
            }
            return result;
        }
        

        public DataTable getDocumentDataHeader(string tableName, List<string> dataRows)
        {
            DataTable result = new DataTable(tableName);
             
            var headerColumns = getDocumentHeaderColumns(tableName);
            foreach (var columnName in headerColumns)
            {
                result.Columns.Add(new DataColumn(columnName.Key, typeof(string)));
            }
            foreach (string dataRow in dataRows)
            {
                string dr = dataRow;
                if (dr.Contains("+++") && dr.Split(Config.COL_SEPARATOR, StringSplitOptions.RemoveEmptyEntries)[0].Contains("+++"))
                {
                    dr = dr.Replace(dr.Split(Config.COL_SEPARATOR, StringSplitOptions.RemoveEmptyEntries)[0], "");
                }
                if (dr.Contains("$$$Шапка:"))//"$$$Шапка:"
                {
                    string[] dataValues = dr.Replace(@" $$$Шапка: $; ", "").Replace(@" $$$ТаблЧасть: ", "").Split(Config.COL_SEPARATOR, StringSplitOptions.RemoveEmptyEntries);
                    if (dataValues.Length + 1 == headerColumns.Count)
                    {
                        DataRow row = result.NewRow();
                        row[0]=getGUIDFromDataRow(dataValues);
                            
                        for (int i = 0; i < headerColumns.Count-1; i++)
                        {
                             var vals=dataValues[i].Split(new string[] {"$$"}, StringSplitOptions.RemoveEmptyEntries);
                             if (vals.Length == 2)
                             {
                                 if (vals[1].Trim().Length > 512)
                                 {
                                     row[i + 1] = vals[1].Trim().Substring(0, 510).Trim();
                                     Logger.logger.Warn(
                                         "getDocumentDataHeader truncate to 512 chars string table:{0}",
                                         tableName);
                                 }else
                                 {
                                     row[i + 1] = vals[1].Trim();
                                 }
                             }
                        }
                        result.Rows.Add(row);
                    }else
                    {
                        Logger.logger.Error(string.Format(
                            "getDocumentDataHeader DB: '{0}' Message: Wrong cols number {1}", _pathDB,
                            tableName));

                    }
                }
            }
             return result;
        }

        private static string getGUIDFromDataRow(string[] dataValues)
        {
            string result;
            if (dataValues.Length > 3)
            {
                result =
                    (dataValues[0].Trim() + dataValues[1].Trim() + dataValues[2].Trim() +
                     dataValues[3].Trim()).StringToGUID().ToString();
            }
            else
            {
                string s = string.Empty;
                foreach (string val in dataValues)
                {
                    s += val.Trim();
                }
                result = s.StringToGUID().ToString();
            }
            return result;
        }

        public DataTable getDocumentDataTable(string tableName, List<string> dataRows)
        {
            DataTable result = new DataTable(tableName);
            var tableColumns = getDocumentTableColumns(tableName);
            try
            {
                foreach (var columnName in tableColumns)
                {
                    result.Columns.Add(new DataColumn(columnName.Key, typeof (string)));
                }
                string hashKey = string.Empty;
                if (dataRows.Count > 1) // skip empty tables
                {
                    foreach (string dataRow in dataRows)
                    {
                        string dr = dataRow;
                        if (dataRow == " $$$ТаблЧаст") // fix 1c code hz...
                        {
                            continue;
                        }
                        if (dr.Contains("+++") &&
                            dr.Split(Config.COL_SEPARATOR, StringSplitOptions.RemoveEmptyEntries)[0].Contains("+++"))
                        {
                            dr = dr.Replace(dr.Split(Config.COL_SEPARATOR, StringSplitOptions.RemoveEmptyEntries)[0], "");
                        }

                        if (dr.Contains("$$$Шапка:"))
                        {
                            hashKey = string.Empty;
                            string[] dataValues =
                                dr.Replace(@" $$$Шапка: $; ", "").Replace(@" $$$ТаблЧасть: ", "").Split(
                                    Config.COL_SEPARATOR,
                                    StringSplitOptions.
                                        RemoveEmptyEntries);
                            hashKey = getGUIDFromDataRow(dataValues);
                        }
                        else if (hashKey != string.Empty)
                        {
                            if (!string.IsNullOrWhiteSpace(dr))
                            {
                                dr = dr.Replace(@" $$$Шапка:  $; ", "").Replace(@" $$$ТаблЧасть: ", "").TrimStart();
                                if (dr.Length > 3 && dr.Substring(0, 3) == "$; ")
                                {
                                    dr = dr.Substring(3, dr.Length - 3);
                                }
                                List<string> tableValues =
                                    new List<string>(dr.Split(Config.COL_SEPARATOR,
                                                              StringSplitOptions.RemoveEmptyEntries));
                                if(tableValues.Count==0)
                                {
                                    continue;
                                }
                                if (string.IsNullOrWhiteSpace(tableValues[0]))
                                {
                                    tableValues.Remove(tableValues[0]);
                                }
                                if (tableValues.Count + 1 == tableColumns.Count)
                                {
                                    DataRow row = result.NewRow();
                                    row[0] = hashKey;
                                    for (int i = 0; i < tableColumns.Count - 1; i++)
                                    {
                                        var vals = tableValues[i].Split(new string[] {"$$"},
                                                                        StringSplitOptions.RemoveEmptyEntries);
                                        if (vals.Length == 2)
                                        {
                                            if (vals[1].Trim().Length > 512)
                                            {
                                                row[i + 1] = vals[1].Trim().Substring(0,510);
                                                Logger.logger.Warn(
                                        "getDocumentDataTable truncate to 512 chars string table:{0}",
                                        tableColumns);
                                            }else
                                            {
                                                row[i + 1] = vals[1].Trim();
                                            }
                                        }
                                    }
                                    result.Rows.Add(row);
                                }
                                else
                                {
                                    Logger.logger.Error(string.Format(
                                        "getDocumentDataTable DB: '{0}' Message: Wrong cols number {1}", _pathDB,
                                        tableName));
                                }
                            }
                        }
                        else
                        {
                            Logger.logger.Debug(
                                string.Format(
                                    "getDocumentDataTable DB: '{0}' Message: Unknown hash key for {1} table data",
                                    _pathDB,
                                    tableName));
                        }
                    }
                }
            }catch(Exception ex)
            {
                Logger.logger.Error(ex);
            }

            return result;
        }

        
        public IList<TableColumn> getDocumentHeaderColumns()
        {
            IList<TableColumn> result = new List<TableColumn>();
            string[] tables = GetDocumentList();
            foreach (string table in tables)
            {
                if (Config.FILTER_TABLES.Exists(p => p == table))
                {
                    var columns = getDocumentHeaderColumns(table);
                    foreach (var column in columns)
                    {
                        var col = new TableColumn()
                                      {
                                          columnName = column.Key,
                                          tableName = string.Format(Config.TABLE_HEADER_FORMAT, table)
                                      };
                        result.Add(col);
                    }
                }
            }
            return result;
        }

        public IList<TableColumn> getDocumentTableColumns()
        {
            IList<TableColumn> result = new List<TableColumn>();
            string[] tables = GetDocumentList();
            foreach (string table in tables)
            {
                if (Config.FILTER_TABLES.Exists(p=>p==table))
                {
                    var columns = getDocumentTableColumns(table);
                    foreach (var column in columns)
                    {
                        var col = new TableColumn()
                                      {
                                          columnName = column.Key,
                                          tableName = string.Format(Config.TABLE_TABLE_FORMAT, table)
                                      };
                        result.Add(col);
                    }
                }
            }
            return result;
        }

        public IList<TableColumn> getCatalogTableColumns()
        {
            IList<TableColumn> result = new List<TableColumn>();
            string[] tables = GetCatalogList();
            foreach (string table in tables)
            {
                if (Config.FILTER_TABLES.Exists(p=>p==table))
                {
                    var columns = getCatalogTableColumns(table);
                    foreach (var column in columns)
                    {
                        var col = new TableColumn() {columnName = column.Key, tableName = table};
                        result.Add(col);
                    }
                }
            }
            return result;
        }

    }
}
