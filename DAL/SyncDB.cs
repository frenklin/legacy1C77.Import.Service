using System;
using Legacy1C77.Import.Service.Core;

namespace Legacy1C77.Import.Service.DAL
{
    public class SyncDB
    {
        public static void InsertSyncLog(SyncLog item)
        {
            using (var context = new DataSyncDataContext())
            {
                try
                {
                    context.SyncLogs.InsertOnSubmit(item);
                    context.SubmitChanges();
                }
                catch (Exception ex)
                {
                    Logger.logger.Error(ex);
                }
            }
        }
    }
}