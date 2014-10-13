using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using Legacy1C77.Import.Service.DAL;
using System.ComponentModel;
using System;

namespace Legacy1C77.Import.Service.Core
{
    public class ImportFrom1Cv77:IDisposable
    {
        private string _userName = string.Empty;
        private string _password = string.Empty;
        private string _pathDB = string.Empty;
        private string _sqlConnectionString = string.Empty;
        private comClient1Cv77Adatpor adaptor1C = null;
        private sqlAdaptor adaptorSql = null;
        private dbf1Cv77Adaptor dbfAdaptor = null;
        private volatile bool _shouldStop = false;
        

        public ImportFrom1Cv77(string sqlConnectionString,string userName, string password, string pathDB)
        {
            this._password = password;
            this._userName = userName;
            this._pathDB = pathDB;
            this._sqlConnectionString = sqlConnectionString;
         
        }

        private void sleepSyncJobSchedule()
        {
            while (!_shouldStop)
            {
                if (DateTime.Now.Hour > 18) // 18-00 24-00 off
                {
                    Thread.Sleep(1000);
                    continue;
                }
                if (Config.SyncDatabases.lastDays.HasValue)
                {// 1 4 7 10 13 16 19 22 
                    if ((DateTime.Now.Hour + 1) % 3 == 0 && DateTime.Now.Minute == 15 && DateTime.Now.DayOfWeek!=DayOfWeek.Saturday && DateTime.Now.DayOfWeek!=DayOfWeek.Sunday)
                    {
                        break;
                    }
                }
                else
                {// 03:40 Saturday full sync start // TODO move to config
                    if (DateTime.Now.Hour == 3 && DateTime.Now.Minute == 40 && DateTime.Now.DayOfWeek==DayOfWeek.Saturday)
                    {
                        break;
                    }
                }
                Thread.Sleep(1000);
            }
        }        

        private void importFrom1C()
        {
            var clock = new Stopwatch();
            var toDatabase = new SqlConnection(_sqlConnectionString).Database;
            var isSuccessfull = false;
            string syncType;
            var syncLog = new SyncLog()
                {
                    ID = Guid.NewGuid(),
                    From = _pathDB,
                    To = toDatabase
                };

            adaptor1C = new comClient1Cv77Adatpor(_userName, _password, _pathDB);
            if (adaptor1C.Login())
            {
                syncLog.Timestamp = DateTime.Now;
                clock.Start();

                if (Config.SyncDatabases.lastDays.HasValue)
                {
                    syncType = "Particular";
                    ParticularImportDataFrom1Cv77 data = new ParticularImportDataFrom1Cv77(adaptor1C, adaptorSql);
                    if (data.Sync())
                    {
                        isSuccessfull = true;
                        Logger.logger.Info(string.Format("Particular Sync OK 1C to MSSQL {0} ", _pathDB));
                    }
                }
                else
                {
                    syncType = "Full";
                    ImportSchemaFrom1Cv77 schema = new ImportSchemaFrom1Cv77(adaptor1C, adaptorSql);
                    if (schema.Sync())
                    {
                        ImportDataFrom1Cv77 data = new ImportDataFrom1Cv77(adaptor1C, adaptorSql);
                        if (data.Sync())
                        {
                            isSuccessfull = true;
                            Logger.logger.Info(string.Format("Full Sync OK 1C to MSSQL {0} ", _pathDB));
                        }
                    }
                }

                clock.Stop();

                syncLog.Description = string.Format("{0} Sync from 1C ({1}) to MSSQL(Database={2})", syncType, _pathDB, toDatabase);
                syncLog.IsSuccessfull = isSuccessfull;
                syncLog.DurationInSec = clock.ElapsedMilliseconds/1000;

                SyncDB.InsertSyncLog(syncLog);
            }
        }

        private void importFromDBF()
        {
            try
            {
                dbfAdaptor = new dbf1Cv77Adaptor(_pathDB);
                ImportDataFromDBF data = new ImportDataFromDBF(dbfAdaptor, adaptorSql);
                if(data.Sync())
                {
                    Logger.logger.Info(string.Format("Full Sync OK DBF to MSSQL {0} ", _pathDB));
                }
                
            }catch(Exception ex)
            {
                Logger.logger.Error(string.Format("importFromDBF DB:'{0}' Ex:{1}", _pathDB, ex.Message));
            }finally
            {
               Dispose(); 
            }
        }

        public void SyncJob(object sender, DoWorkEventArgs e)
        {
            while (!_shouldStop)
            {
                try
                {
                    adaptorSql = new sqlAdaptor(_sqlConnectionString);
                    if (adaptorSql.State == ConnectionState.Open)
                    {
                        //if (false) // move to config sync type now actual only sync from 1C
                        //{
                        //    importFromDBF();
                        //}else{
                            lock (ImportService.lock1C)
                            {
                                importFrom1C();
                            }
                        //}
                    }
                }
                finally
                {
                    Dispose();
                }
                sleepSyncJobSchedule();
            }
        }

        public void Dispose()
        {
            try
            {
                if (adaptorSql != null)
                {
                    adaptorSql.Dispose();
                }
                if (adaptor1C != null)
                {
                    adaptor1C.Dispose();
                }
                if (dbfAdaptor != null)
                {
                    dbfAdaptor.Dispose();
                }
                dbfAdaptor = null;
                adaptor1C = null;
                adaptorSql = null;
            }catch(Exception ex)
            {
                Logger.logger.Error(ex);
            }
        }

        public void StopRequest()
        {
            _shouldStop = true;
            Dispose();
        }
    }

}
