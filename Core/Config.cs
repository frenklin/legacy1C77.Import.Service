using System.Configuration;
using System;
using System.Collections.Generic;

namespace Legacy1C77.Import.Service.Core
{
    public static class Config
    {
        public static List<string> FILTER_TABLES = new List<string>() {"ВозвратПоставщику", "РасходныйКассовый", "РасходнаяНакладная", "ПриходнаяНакладная",
                                                             "ПриходныйКассовый", "БанковскаяВыписка","Контрагенты", "ПлатежноеПоручение", "ТМЦ"};
        //public static List<string> FILTER_TABLES = new List<string>() {"РасходнаяНакладная", "ПриходнаяНакладная",
        //                                                     };
        
        public static string[] COL_SEPARATOR = new string[] { "$;" };
        public static string[] LINE_SEPARATOR = new string[] {"#|"};
        public static string[] TABLE_SEPARATOR = new string[] {"$$$Табл:"};
        public static string[] COL_INFO = new string[] { "@" };
        public static string TABLE_HEADER_FORMAT = "{0}Шапка";
        public static string TABLE_TABLE_FORMAT = "{0}Табл";
        public static string[] DDCOL_SEPARATOR = new string[] {"|"};
        public static SyncDatabasesList SyncDatabases = (SyncDatabasesList)ConfigurationManager.GetSection("Enterprise.Import.Service.Core.Config");
        public static int COMMAND_TIMEOUT = 900;
    }

    public class ServerElement : ConfigurationElement
    {
        [ConfigurationProperty("DB1CPath", IsKey = true, IsRequired = true)]
        public string DB1CPath
        {
            get { return (string) this["DB1CPath"]; }
        }

        [ConfigurationProperty("DB1CLogin", IsRequired = true)]
        public string DB1CLogin
        {
            get { return (string)this["DB1CLogin"]; }
        }

        [ConfigurationProperty("DB1CPassword", IsRequired = true)]
        public string DB1CPassword
        {
            get { return (string)this["DB1CPassword"]; }
        }

        [ConfigurationProperty("SqlConnectionString", IsRequired = true)]
        public string SqlConnectionString
        {
            get { return (string)this["SqlConnectionString"]; }
        }
    }

    public class ServerElementCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new ServerElement();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((ServerElement)element).DB1CPath;
        }
    }

    public class SyncDatabasesList : ConfigurationSection
    {
        [ConfigurationProperty("lastDays", DefaultValue = null, IsRequired = false)]
        public int? lastDays 
        {
            get { return (int?)this["lastDays"]; }
        }

        [ConfigurationProperty("Databases")]
        public ServerElementCollection Servers
        {
            get { return (ServerElementCollection)this["Databases"]; }
        }
    }
}
