using MySql.Data.MySqlClient;
using Server.Tools;
using System.Collections.Concurrent;

namespace GameDBServer.DB
{
    /// <summary>
    /// Quản lý kết nối DB
    /// </summary>
    public class DBConnections
    {
        /// <summary>
        /// Tên DB
        /// </summary>
        public static string dbNames = "";

        /// <summary>
        /// Đối tượng Semaphore
        /// </summary>
        private Semaphore? SemaphoreClients = null;

        /// <summary>
        /// Danh sách kết nối đang chờ
        /// </summary>
        private ConcurrentQueue<MySqlConnection> DBConns = new ConcurrentQueue<MySqlConnection>();

        /// <summary>
        /// MUTEX dùng trong LOCK
        /// </summary>
        private object Mutex = new object();

        /// <summary>
        /// Chuỗi kết nói
        /// </summary>
        private string? ConnectionString;

        /// <summary>
        /// Tổng số kết nối
        /// </summary>
        private int CurrentCount;

        /// <summary>
        /// Kết nối tối đa
        /// </summary>
        private int MaxCount;

        /// <summary>
        /// Tạo kết nối mới tới Database
        /// </summary>
        /// <param name="connStr"></param>
        public void BuidConnections(MySqlConnectionStringBuilder connStr, int maxCount)
        {
            //lock (this.Mutex)
            {
                ConnectionString = connStr.ConnectionString;
                MaxCount = maxCount;
                SemaphoreClients = new Semaphore(0, MaxCount);

                for (int i = 0; i < MaxCount; i++)
                {
                    MySqlConnection? dbConn = CreateAConnection();
                    if (null == dbConn)
                    {
                        throw new Exception(string.Format("Connect to MySQL faild"));
                    }
                }
            }
        }

        /// <summary>
        /// Tạo kết nối
        /// </summary>
        /// <returns></returns>
        private MySqlConnection? CreateAConnection()
        {
            try
            {
                var dbConn = new MySqlConnection(ConnectionString);
                dbConn.Open();
                if (!string.IsNullOrEmpty(dbNames))
                {
                    using (MySqlCommand cmd = new MySqlCommand(string.Format("SET names '{0}'", dbNames), dbConn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }

                DBConns.Enqueue(dbConn);
                CurrentCount++;
                this.SemaphoreClients?.Release();

                return dbConn;
            }
            catch (Exception ex)
            {
                LogManager.WriteLog(LogTypes.Exception, string.Format("Create database connection exception: \r\n{0} \r\n{1}", ex.ToString(), ConnectionString));
            }

            return null;
        }

        /// <summary>
        /// Duy trì kết nối
        /// </summary>
        /// <returns></returns>
        public bool SupplyConnections()
        {
            if (CurrentCount < MaxCount)
            {
                var conn = CreateAConnection();
                return conn != null;

            }

            return false;
        }

        /// <summary>
        /// Trả về tổng số kết nối đến Database
        /// </summary>
        /// <returns></returns>
        public int GetDBConnsCount()
        {
            return this.DBConns.Count;
        }

        /// <summary>
        /// Lấy kết nối đến Database trong hàng đợi để thực thi
        /// </summary>
        /// <returns></returns>
        public MySqlConnection? PopDBConnection()
        {
            MySqlConnection? conn = null;
            bool lost = false;

            do
            {
                string cmdText = @"select 1";
                lost = true;
                SemaphoreClients?.WaitOne();

                /// Toác
                if (!this.DBConns.TryDequeue(out conn))
                {
                    return null;
                }

                try
                {
                    using (MySqlCommand cmd = new MySqlCommand(cmdText, conn))
                    {
                        try
                        {
                            cmd.ExecuteNonQuery();
                            lost = false;
                        }
                        catch (System.Exception ex)
                        {
                            LogManager.WriteLog(LogTypes.Exception, string.Format("Exception occurred when executing Database Query: {0}\r\n{1}", cmdText, ex.ToString()));
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogManager.WriteException(ex.ToString());
                }
                finally
                {
                    if (lost)
                    {
                        try
                        {
                            conn?.Dispose();
                        }
                        catch (Exception ex)
                        {
                            LogManager.WriteLog(LogTypes.Exception, string.Format("Exception occurred when disposing Database connection: {0}\r\n{1}", ConnectionString, ex.ToString()));
                        }
                        finally
                        {

                        }
                        CurrentCount--;
                    }
                }
            }
            while (lost);

            return conn;
        }

        /// <summary>
        /// Thực thi kết nối
        /// </summary>
        /// <param name="conn"></param>
        public void PushDBConnection(MySqlConnection? conn)
        {
            if (null != conn)
            {
                //lock (this.Mutex)
                {
                    this.DBConns.Enqueue(conn);
                }

                SemaphoreClients?.Release();
            }
        }
    }
}
