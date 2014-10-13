using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Data.OleDb;
using Legacy1C77.Import.Service.Core;
using System.IO;

namespace Legacy1C77.Import.Service.DAL
{

    public class dbf1Cv77Adaptor:IDisposable
    {
        private OleDbConnection _connection = null;
        private string _pathDB = string.Empty;
        private enConnectionState _State = enConnectionState.Closed;
        private List<TableColumn> _tablesDescribe = null;
        private static Encoding encWin1251=Encoding.GetEncoding("windows-1251");
        private static Encoding enc866=Encoding.GetEncoding("cp866");

        public enConnectionState State
        {
            get { return _State; }
        }

        public List<TableColumn> TablesDescribe
        {
            get { return _tablesDescribe; }
        }

        public dbf1Cv77Adaptor(string pathDB)
        {
            this._pathDB = pathDB;
            this._tablesDescribe = new List<TableColumn>();
            Initialize();
        }

        private void Initialize()
        {
            try
            {
                string fullFileName = Path.Combine(_pathDB, "1Cv7.DD");
                if(File.Exists(fullFileName))
                {
                    string[] fileDescribe = File.ReadAllLines(_pathDB + "1Cv7.DD", Encoding.GetEncoding("windows-1251"));
                    LoadDescribeDB(fileDescribe);

                    _connection =
                        new OleDbConnection(@"Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + _pathDB +
                                            @";Extended Properties=""dBASE IV;Exclusive=No;""");
                    _connection.Open();
                    _State = enConnectionState.Connected;
                }else
                {
                    throw new Exception(string.Format("Can't initialize dbf1Cv77Adaptor {0} not found", fullFileName));
                }
            }
            catch (Exception e)
            {
                Logger.logger.Error("Can't initialize dbf1Cv77Adaptor" + e.Message);
                throw;
            }
            Logger.logger.Debug("Initialize dbf1Cv77Adaptor");
        }

        public DataTable getDataTable(string tableName)
        {
            OleDbDataReader reader = null;
            DataTable dt = new DataTable(tableName);
            OleDbCommand command = null;

            try
            {
                string tableNameSystem = _tablesDescribe.First(t => t.tableName.Equals(tableName)).tableNameSystem;
                command=new OleDbCommand("select * from " + tableNameSystem + ".DBF", _connection);
                reader = command.ExecuteReader();
                
                foreach(TableColumn tc in _tablesDescribe.Where(t=>t.tableName.Equals(tableName)))
                {
                    dt.Columns.Add(new DataColumn(tc.columnName));
                }

                while(reader.Read())
                {
                    DataRow dr = dt.NewRow();
                    foreach (TableColumn tc in _tablesDescribe.Where(t => t.tableName.Equals(tableName)))
                    {
                        object val = reader[tc.columnNameSystem];
                        if (val is string)
                        {
                            dr[tc.columnName] = val.ToString().Trim().Encode(encWin1251, enc866);
                        }
                        else
                        {
                            dr[tc.columnName].ToString().Trim();
                        }
                    }
                    dt.Rows.Add(dr);
                }
            }catch(Exception ex)
            {
                Logger.logger.Error(ex);
            }
            finally
            {
                if(reader!=null)
                {
                    reader.Close();
                    reader.Dispose();
                }
                reader = null;
            }
            //int i = ds.Tables[0].Rows.Count;
            return dt;
        }

        private void LoadDescribeDB(string[] dbDescribe)
        {
            string line = string.Empty;
            string colName = string.Empty;
            string tblName = string.Empty;
            string tblSysName = string.Empty;
            _tablesDescribe.Clear();
            foreach(string lineDescr in dbDescribe)
            {
                line = lineDescr.Trim();
                if(line.Trim().Length==0 || line.Trim()[0]=='#')
                {
                    continue;
                }
                if(line.Substring(0,2)=="T=")
                {
                    string[] tableInfo = line.Split(Config.DDCOL_SEPARATOR, StringSplitOptions.None);
                    tblSysName = tableInfo[0].Substring(2, tableInfo[0].Length-2).Trim();
                    tblName = tblSysName + tableInfo[1].Trim();
                    continue;
                }
                if (line.Substring(0, 2) == "F=")
                {
                    string[] colInfo = line.Split(Config.DDCOL_SEPARATOR, StringSplitOptions.None);
                    
                    //if (!tblSysName.Contains("1S"))
                    {
                        //test
                        if (colInfo[1].Contains("(P)"))
                        {
                            colName = colInfo[0].Substring(2, colInfo[0].Length - 2).Trim() +
                                      colInfo[1].Trim().Replace("(P)", "");
                        }
                        else
                        {
                            colName = colInfo[0].Substring(2, colInfo[0].Length - 2).Trim();
                        }
                        if (!_tablesDescribe.Any(t => t.tableNameSystem.Equals(tblSysName) && t.columnName.Equals(colName)))
                        {
                            _tablesDescribe.Add(new TableColumn()
                                                    {
                                                        columnName = colName,
                                                        columnNameSystem =
                                                            colInfo[0].Substring(2, colInfo[0].Length - 2).Trim(),
                                                        tableName = tblName,
                                                        tableNameSystem = tblSysName,
                                                        dataType = "nvarchar(256)",
                                                        length = 256
                                                    });
                        }
                    }
                }
            }
        }

        public void Dispose()
        {
            if (_connection != null)
            {
                if (_connection.State == ConnectionState.Open)
                {
                    _connection.Close();
                }
                _connection.Dispose();
            }
            _connection = null;
            _State = enConnectionState.Closed;
        }
    }
}
