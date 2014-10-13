using System;
using System.Collections.Generic;
using System.ServiceProcess;
using System.Threading;
using Legacy1C77.Import.Service.Core;
using System.ComponentModel;

namespace Legacy1C77.Import.Service
{
    public partial class ImportService : ServiceBase
    {

        private static Dictionary<string, Tuple<ImportFrom1Cv77, BackgroundWorker, DoWorkEventHandler>> threads = new Dictionary<string, Tuple<ImportFrom1Cv77, BackgroundWorker, DoWorkEventHandler>>();
        public static object lock1C = new object();

        public ImportService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            startSync();
            base.OnStart(args);
        }

        public static void startSync()
        {
            if (threads.Count > 0)
            {
                Thread.Sleep(1000 * 60 * 10); // timeout thread stop Wait
            }
            foreach (ServerElement server in Config.SyncDatabases.Servers)
            {
                if (!threads.ContainsKey(server.DB1CPath))
                {
                    BackgroundWorker proc = new BackgroundWorker();
                    proc.WorkerSupportsCancellation = true;
                    var importFrom1Cv77 = new ImportFrom1Cv77(server.SqlConnectionString, server.DB1CLogin, server.DB1CPassword, server.DB1CPath);
                    DoWorkEventHandler task = new DoWorkEventHandler(importFrom1Cv77.SyncJob);
                    proc.DoWork += task;
                    proc.RunWorkerAsync();

                    threads.Add(server.DB1CPath, new Tuple<ImportFrom1Cv77, BackgroundWorker, DoWorkEventHandler>(importFrom1Cv77, proc, task));
                    Logger.logger.Info(string.Format("Thread ImportFrom1Cv77 Create from DB:'{0}'", server.DB1CPath));
                }
            }
            Logger.logger.Info("Service Enterprise.Import.Service Started");
        }

        public static void stopSync()
        {

            foreach (KeyValuePair<string, Tuple<ImportFrom1Cv77, BackgroundWorker, DoWorkEventHandler>> thread in threads)
            {
                if (thread.Value.Item1 != null)
                {
                    thread.Value.Item1.StopRequest();
                    thread.Value.Item1.Dispose();
                }
                thread.Value.Item2.CancelAsync();
                if (thread.Value.Item2 != null)
                {
                    thread.Value.Item2.Dispose();
                }
                Logger.logger.Info(string.Format("Thread ImportFrom1Cv77 Stop from DB:'{0}'", thread.Key));
            }
            threads.Clear();
            GC.Collect();
            Logger.logger.Info("Stop Enterprise.Import.Service");

        }

        protected override void OnStop()
        {
            try
            {
                stopSync();
            }
            catch (Exception ex)
            {
                Logger.logger.Error(ex);
            }
        }

        protected override void OnShutdown()
        {
            try
            {
                stopSync();
            }
            catch (Exception ex)
            {
                Logger.logger.Error(ex);
            }
        }
    }
}
