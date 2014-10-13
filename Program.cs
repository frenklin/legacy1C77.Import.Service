using System;
using System.Configuration.Install;
using System.Reflection;
using System.ServiceProcess;

namespace Legacy1C77.Import.Service
{
    static class program
    {
        public static void ExecuteCommandSync(string command)
        {
            try
            {
                // create the ProcessStartInfo using "cmd" as the program to be run,
                // and "/c " as the parameters.
                // Incidentally, /c tells cmd that we want it to execute the command that follows,
                // and then exit.
                System.Diagnostics.ProcessStartInfo procStartInfo =
                    new System.Diagnostics.ProcessStartInfo("cmd", "/c " + command);

                // The following commands are needed to redirect the standard output.
                // This means that it will be redirected to the Process.StandardOutput StreamReader.
                procStartInfo.RedirectStandardOutput = true;
                procStartInfo.UseShellExecute = false;
                // Do not create the black window.
                procStartInfo.CreateNoWindow = true;
                // Now we create a process, assign its ProcessStartInfo and start it
                System.Diagnostics.Process proc = new System.Diagnostics.Process();
                proc.StartInfo = procStartInfo;
                proc.Start();
                // Get the output into a string
                string result = proc.StandardOutput.ReadToEnd();
                // Display the command output.
                Console.WriteLine(result);
            }
            catch (Exception objException)
            {
                // Log the exception
            }
        }

        internal static readonly string ExePath = Assembly.GetExecutingAssembly().Location;
        public static bool Install()
        {
            try
            {
                ManagedInstallerClass.InstallHelper(new[] { ExePath });
                //allow listen server port 9931
                //ExecuteCommandSync("netsh http add urlacl url=http://+:9931/ user=\"NETWORK SERVICE\"");
            }
            catch { return false; }
            return true;
        }
        public static bool Uninstall()
        {
            try { ManagedInstallerClass.InstallHelper(new[] { "/u", ExePath }); }
            catch { return false; }
            return true;
        }

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            if (args != null && args.Length == 1 && args[0].Length > 1
            && (args[0][0] == '-' || args[0][0] == '/'))
            {
                switch (args[0].Substring(1).ToLower())
                {
                    case "install":
                    case "i":
                        if (!Install())
                            Console.WriteLine("Failed to install service");
                        break;
                    case "uninstall":
                    case "u":
                        if (!Uninstall())
                            Console.WriteLine("Failed to uninstall service");
                        break;
                    case "r":
                    case "run":
                        Run();
                        break;
                    default:
                        Console.WriteLine("Unrecognized parameters. Usage: /i /install /u /uninstall /r /run");
                        break;
                }
            }
            else
            {
                ServiceBase[] servicesToRun=null;
                servicesToRun = new ServiceBase[] 
			    { 
				    new ImportService(),  
			    };
                ServiceBase.Run(servicesToRun);
            }
        }

        private static void Run()
        {
            Console.CancelKeyPress+=new ConsoleCancelEventHandler(Console_CancelKeyPress);
            Console.WriteLine("Press 'X' to quit");
           
            ImportService.startSync();
            while(true)
            {
                if(Console.ReadKey(true).Key == ConsoleKey.X)
                {
                    Console_CancelKeyPress(null, null);
                    break;
                };
            }
        }

        static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {      
                ImportService.stopSync();
                Console.WriteLine("Sync Stop");
        }
    }
}
