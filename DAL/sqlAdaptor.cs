using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Legacy1C77.Import.Service.Core;

namespace Legacy1C77.Import.Service.DAL
{
    public class sqlAdaptor:IDisposable
    {
        public ConnectionState State
        {
            get { return sqlConnect.State; }
        }

        public sqlAdaptor(string connectionString)
        {
            this._connectionString = connectionString;
        }

        private string _connectionString = string.Empty;
       SqlConnection _sqlConnect = null;
       SqlConnection sqlConnect
       {
           get
           {
               if (_sqlConnect == null)
               {
                   _sqlConnect = new SqlConnection();
               }
               if (_sqlConnect.State != ConnectionState.Open)
               {
                   _sqlConnect.ConnectionString = _connectionString;
                   try
                   {
                       _sqlConnect.Open();
                   }
                   catch (Exception ex) { 
                    Logger.logger.Error(ex);
                   }
               }
               return _sqlConnect;
           }
       }

		protected int ExecuteNonQuery(string query)
		{
			_sqlConnect = null;
			SqlTransaction sqt = sqlConnect.BeginTransaction();
			int res = 0;
			SqlCommand sc = null;
			try
			{
				sc = new SqlCommand(query, sqlConnect, sqt);
				sc.CommandTimeout = Config.COMMAND_TIMEOUT;
				res = sc.ExecuteNonQuery();
				sqt.Commit();
			}
			catch (Exception ex)
			{
				try
				{
					sqt.Rollback();
					Logger.logger.Error("Commit exception: " + ex);
				}
				catch (Exception rollbackEx)
				{
					Logger.logger.Error("Rollback exception: " + rollbackEx);
				}
			}
			finally
			{
				if (sc != null)
				{
					sc.Dispose();
				}
				sqt.Dispose();
			}
			return res;
		}

		protected object ExecuteQuery(string query)
		{
			SqlTransaction sqt = sqlConnect.BeginTransaction();
			SqlCommand sc = null;
			object res = null;
			try
			{
				sc = new SqlCommand(query, sqlConnect, sqt);
				sc.CommandTimeout = Config.COMMAND_TIMEOUT;
				res = sc.ExecuteScalar();
				sqt.Commit();
			}
			catch (Exception ex)
			{
				try
				{
					sqt.Rollback();
					Logger.logger.Error("Commit exception: " + ex);
				}
				catch (Exception rollbackEx)
				{
					Logger.logger.Error("Rollback exception: " + rollbackEx);
				}
			}
			finally
			{
				if (sc != null)
				{
					sc.Dispose();
				}
			}
			return res;
		}

        public IList<TableColumn> getTableColumns()
        {
            IList<TableColumn> result= new List<TableColumn>();
            string query = @"SELECT DISTINCT sysobjects.name AS table_name, syscolumns.name AS column_name, systypes.name AS datatype, syscolumns.LENGTH,syscolumns.colid AS colid
                            FROM         sysobjects INNER JOIN
                                         syscolumns ON sysobjects.id = syscolumns.id INNER JOIN
                                         systypes ON syscolumns.xtype = systypes.xtype
                            WHERE     (sysobjects.xtype = 'U') and systypes.name<>'sysname'
                            ORDER BY sysobjects.name, syscolumns.colid";
            SqlDataReader reader = null;
            SqlCommand sc = null;
            try
            {
                sc = new SqlCommand(query, sqlConnect);
                sc.CommandTimeout = Config.COMMAND_TIMEOUT;
                reader = sc.ExecuteReader();
                while (reader.Read())
                {
                    result.Add(new TableColumn()
                                   {
                                       columnName = reader["column_name"].ToString(),
                                       tableName = reader["table_name"].ToString(),
                                       dataType = reader["datatype"].ToString(),
                                       length = int.Parse(reader["LENGTH"].ToString())
                                   });
                }
            }
            catch (Exception ex)
            {
                Logger.logger.Error(ex);
                throw;
            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }
                if(sc!=null)
                {
                    sc.Dispose();
                }
            }
            return result;
        }

        public void bulkInsertData(string tableName, DataTable table )
        {
            if (table.Rows.Count > 0)
            {
                SqlBulkCopy sbc = null;
                try
                {

                    sbc = new SqlBulkCopy(_connectionString, SqlBulkCopyOptions.TableLock);
                    sbc.BulkCopyTimeout = Config.COMMAND_TIMEOUT;
                    sbc.DestinationTableName = "["+tableName+"]";
                    sbc.WriteToServer(table);
                }
                catch (Exception ex)
                {
                    Logger.logger.Error(ex);
                }
                finally
                {
                    if (sbc != null)
                    {
                        sbc.Close();
                    }
                    sbc = null;
                }
            }
        }
        /* OBSOLETTE USE bulk insert it's faster
        public void insertData(string tableName, DataTable table )
        {
            if (table.Rows.Count > 0)
            {
                string tableFields = string.Empty;
                foreach (var column in table.Columns)
                {
                    if (tableFields != string.Empty)
                    {
                        tableFields += ",";
                    }
                    tableFields += string.Format("[{0}]", column);
                }
                StringBuilder dataSelect = new StringBuilder();
                StringBuilder dataValues = new StringBuilder();
                string query = string.Empty;
                int i = 0;
                foreach (DataRow row in table.Rows)
                {
                    if (dataSelect.Length > 0)
                    {
                        dataSelect.Append(",");
                    }
                    dataValues.Clear();
                    foreach (DataColumn column in table.Columns)
                    {
                        if (dataValues.Length > 0)
                        {
                            dataValues.Append(",");
                        }
                        dataValues.Append(string.Format("'{0}'",
                                                        row[column.ColumnName].ToString().Trim().Replace("'", "''")));
                    }
                    dataSelect.Append(string.Format("({0})", dataValues));
                    if ((i+1)%998==0)
                    {
                        query = string.Format("INSERT INTO [{0}] WITH(TABLOCK) ({1}) VALUES {2}", tableName, tableFields, dataSelect);
                        ExecuteNonQuery(query);
                        dataSelect.Clear();
                        GC.Collect();
                    }
                    i++;
                }
                if (dataSelect.Length > 0)
                {
                    query = string.Format("INSERT INTO [{0}] WITH(TABLOCK) ({1}) VALUES {2}", tableName, tableFields, dataSelect);
                    ExecuteNonQuery(query);
                }
            }
        }
        */
        public void truncateTable(string tableName)
        {
            ExecuteNonQuery(string.Format("TRUNCATE TABLE [{0}]", tableName));
        }

        public void recreateTable(string tableName, IList<TableColumn> columns)
        {
            if (getTableColumns().Any(c => c.tableName.Equals(tableName)))
            {
                dropTable(tableName);
            }
            createTable(tableName, columns);
        }

        public void renameTable(string oldName, string newName)
        {
            if (getTableColumns().Any(s => s.tableName == oldName))
            {
                ExecuteNonQuery(string.Format("exec sp_rename '[{0}]', '{1}'", oldName, newName));
            }
        }

        private void createTable(string tableName, IList<TableColumn> columns)
        {
            string fieldsSql=string.Empty;
            foreach (var tableColumn in columns)
            {
                if(fieldsSql!=string.Empty)
                {
                    fieldsSql += string.Format(",");
                }
                fieldsSql += string.Format("[{0}] {1}", tableColumn.columnName, "nvarchar(512) NULL");
            }
            string query = string.Format("CREATE TABLE [{0}] ({1}) ON [PRIMARY]", tableName, fieldsSql);
            ExecuteNonQuery(query);
        }

        public bool dropTable(string tableName)
        {
            if (getTableColumns().Any(s => s.tableName == tableName))
            {
                ExecuteNonQuery(string.Format("DROP TABLE [{0}]", tableName));
                return true;
            }
            return false;
        }
        
        public bool deleteRefTable(string tableName, string tableDocName, DateTime fromDate)
        {
            if (getTableColumns().Any(s => s.tableName == tableName && s.columnName.Equals("ДатаДок")))
            {
                ExecuteNonQuery(string.Format("delete de1 FROM [{1}] de1 inner join [{0}] de2 ON de1.hashKey = de2.hashKey where CONVERT(date, de2.[ДатаДок], 4)>='{2}'", tableName,
                                              tableDocName, fromDate.ToString("yyyyMMdd")));
                return true;
            }
            return false;
        }

        public bool deleteTable(string tableName, DateTime fromDate)
        {
            if (getTableColumns().Any(s => s.tableName == tableName && s.columnName.Equals("ДатаДок")))
            {
                ExecuteNonQuery(string.Format("DELETE [{0}] WHERE CONVERT(date, [ДатаДок], 4)>='{1}'"
                    , tableName,
                                              fromDate.ToString("yyyyMMdd")));
                return true;
            }
            return false;
        }

        public void Dispose()
        {
            if(_sqlConnect!=null)
            {
                _sqlConnect.Dispose();
            }
            _sqlConnect = null;
        }
    }
}
