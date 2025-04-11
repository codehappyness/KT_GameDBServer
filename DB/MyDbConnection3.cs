using System.Data;
using MySql.Data.MySqlClient;
using Server.Tools;
using GameDBServer.Logic;

namespace GameDBServer.DB
{
#if true
    public enum EffentNextType
    {
        /// <summary>
        /// 对其他语句无任何影响 
        /// </summary>
        None,
        /// <summary>
        /// 当前语句必须为"select count(1) from .."格式，如果存在则继续执行，不存在回滚事务
        /// </summary>
        WhenHaveContine,
        /// <summary>
        /// 当前语句必须为"select count(1) from .."格式，如果不存在则继续执行，存在回滚事务
        /// </summary>
        WhenNoHaveContine,
        /// <summary>
        /// 当前语句影响到的行数必须大于0，否则回滚事务
        /// </summary>
        ExcuteEffectRows,
        /// <summary>
        /// 引发事件-当前语句必须为"select count(1) from .."格式，如果不存在则继续执行，存在回滚事务
        /// </summary>
        SolicitationEvent
    }
    public class CommandInfo
    {
        public object? ShareObject = null;
        public object? OriginalData = null;
        event EventHandler _solicitationEvent;
        public event EventHandler SolicitationEvent
        {
            add
            {
                _solicitationEvent += value;
            }
            remove
            {
                _solicitationEvent -= value;
            }
        }
        public void OnSolicitationEvent()
        {
            if (_solicitationEvent != null)
            {
                _solicitationEvent(this, new EventArgs());
            }
        }
        public string CommandText;
        public MySqlParameter[] Parameters;
        public EffentNextType EffentNextType = EffentNextType.None;
        public CommandInfo()
        {

        }
        public CommandInfo(string sqlText, MySqlParameter[] para)
        {
            this.CommandText = sqlText;
            this.Parameters = para;
        }
        public CommandInfo(string sqlText, MySqlParameter[] para, EffentNextType type)
        {
            this.CommandText = sqlText;
            this.Parameters = para;
            this.EffentNextType = type;
        }
    }

#endif

    public class MyDbConnection3 : IDisposable
    {
        public MySqlConnection? DbConn = null;
        private MySqlDataReader _MySQLDataReader;
        public static bool LogSQLString = true;

        private bool m_disposed = false;

        public MyDbConnection3()
        {
            _MySQLDataReader = null;
            DbConn = DBManager.getInstance().DBConns.PopDBConnection();
        }

        void IDisposable.Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool isDisposing)
        {
            if (!m_disposed)
            {
                if (isDisposing)
                {
                    if (_MySQLDataReader != null && !_MySQLDataReader.IsClosed)
                    {
                        _MySQLDataReader.Close();
                        _MySQLDataReader = null;
                    }

                    DBManager.getInstance().DBConns.PushDBConnection(DbConn);
                }

                m_disposed = true;
            }
        }

        public void Close()
        {
            ((IDisposable)this).Dispose();
        }

        private void LogSql(string sqlText)
        {
            if (LogSQLString)
            {
                GameDBManager.SystemServerSQLEvents.AddEvent(string.Format("+SQL: {0}", sqlText), EventLevels.Important);
            }
        }

        public bool ExecuteNonQueryBool(string sql, int commandTimeout = 0)
        {
            return ExecuteNonQuery(sql, commandTimeout) >= 0;
        }

        public int ExecuteNonQuery(string sql, int commandTimeout = 0)
        {
            int result = -1;

            try
            {
                using (MySqlCommand cmd = new MySqlCommand(sql, DbConn))
                {
                    if (commandTimeout > 0)
                    {
                        cmd.CommandTimeout = commandTimeout;
                    }

                    result = cmd.ExecuteNonQuery();
                    LogSql(sql);
                }
            }
            catch (System.Exception ex)
            {
                LogManager.WriteLog(LogTypes.Exception, string.Format("SQL BUGGGG: {0}\r\n{1}", sql, ex.ToString()));
                LogManager.WriteLog(LogTypes.Error, string.Format("BUG SQL: {0}", sql));
                result = -1;
            }

            return result;
        }

        public int ExecuteWithContent(string sql, string content)
        {
            int result = 0;

            try
            {
                using (MySqlCommand cmd = new MySqlCommand(sql, DbConn))
                {
                    MySqlParameter myParameter = new MySqlParameter("@content", content);
                    //myParameter.Value = content;
                    cmd.Parameters.Add(myParameter);

                    result = cmd.ExecuteNonQuery();
                    LogSql(sql);
                }
            }
            catch (System.Exception ex)
            {
                LogManager.WriteLog(LogTypes.Exception, string.Format("执行SQL异常: {0}\r\n{1}", sql, ex.ToString()));
                LogManager.WriteLog(LogTypes.Error, string.Format("写入数据库失败: {0}", sql));
                result = -1;
            }

            return result;
        }

        public int GetSingleInt(string sql, int commandTimeout = 0, params MySqlParameter[] cmdParms)
        {
            object obj = GetSingle(sql, commandTimeout, cmdParms);
            return Convert.ToInt32(obj.ToString());
        }

        public long GetSingleLong(string sql, int commandTimeout = 0, params MySqlParameter[] cmdParms)
        {
            object obj = GetSingle(sql, commandTimeout, cmdParms);
            return Convert.ToInt64(obj.ToString());
        }

        public object GetSingle(string sql, int commandTimeout = 0, params MySqlParameter[] cmdParms)
        {
            try
            {
                using (MySqlCommand cmd = new MySqlCommand(sql, DbConn))
                {
                    if (commandTimeout > 0)
                    {
                        cmd.CommandTimeout = commandTimeout;
                    }

                    if (cmdParms.Length > 0)
                    {
                        PrepareCommand(cmd, DbConn, null, sql, cmdParms);
                    }

                    object obj = cmd.ExecuteScalar();
                    if (cmdParms.Length > 0)
                    {
                        cmd.Parameters.Clear();
                    }

                    LogSql(sql);
                    if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                    {
                        return null;
                    }
                    else
                    {
                        return obj;
                    }
                }
            }
            catch (System.Exception ex)
            {
                LogManager.WriteLog(LogTypes.Exception, string.Format("执行SQL异常: {0}\r\n{1}", sql, ex.ToString()));
                LogManager.WriteLog(LogTypes.Error, string.Format("写入数据库失败: {0}", sql));
            }

            return null;
        }

        public object ExecuteSqlGet(string sql, string content)
        {
            try
            {
                using (MySqlCommand cmd = new MySqlCommand(sql, DbConn))
                {
                    MySqlParameter myParameter = new MySqlParameter("@content", content);
                    //myParameter.Value = content;
                    cmd.Parameters.Add(myParameter);

                    object obj = cmd.ExecuteScalar();
                    LogSql(sql);
                    if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                    {
                        return null;
                    }
                    else
                    {
                        return obj;
                    }
                }
            }
            catch (System.Exception ex)
            {
                LogManager.WriteLog(LogTypes.Exception, string.Format("执行SQL异常: {0}\r\n{1}", sql, ex.ToString()));
                LogManager.WriteLog(LogTypes.Error, string.Format("写入数据库失败: {0}", sql));
            }

            return null;
        }

        public MySqlDataReader? ExecuteReader(string sql, params MySqlParameter[] cmdParms)
        {
            try
            {
                using (MySqlCommand cmd = new MySqlCommand(sql, DbConn))
                {
                    if (cmdParms.Length > 0)
                    {
                        PrepareCommand(cmd, DbConn, null, sql, cmdParms);
                    }

                    //MySQLDataReader myReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                    MySqlDataReader myReader = cmd.ExecuteReader();
                    if (cmdParms.Length > 0)
                    {
                        cmd.Parameters.Clear();
                    }

                    _MySQLDataReader = myReader;
                    LogSql(sql);

                    return myReader;
                }
            }
            catch (System.Exception ex)
            {
                LogManager.WriteLog(LogTypes.Exception, string.Format("执行SQL异常: {0}\r\n{1}", sql, ex.ToString()));
                LogManager.WriteLog(LogTypes.Error, string.Format("写入数据库失败: {0}", sql));
            }

            return null;
        }

        public DataSet Query(string sql, int Times = 0)
        {
            DataSet ds = new DataSet();
            MySqlDataAdapter command = null;

            using (command = new MySqlDataAdapter(sql, DbConn))
            {
                try
                {
                    if (Times > 0)
                    {
                        command.SelectCommand.CommandTimeout = Times;
                    }

                    command.Fill(ds, "ds");
                    LogSql(sql);
                }
                catch (MySqlException ex)
                {
                    LogManager.WriteLog(LogTypes.Exception, string.Format("执行SQL异常: {0}\r\n{1}", sql, ex.ToString()));
                    LogManager.WriteLog(LogTypes.Error, string.Format("写入数据库失败: {0}", sql));
                }
            }

            return ds;
        }

        public DataSet Query(string sql, params MySqlParameter[] cmdParms)
        {
            DataSet ds = new DataSet();
            MySqlDataAdapter command = null;

            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, DbConn, null, sql, cmdParms);
            using (MySqlDataAdapter da = new MySqlDataAdapter(cmd))
            {
                try
                {
                    da.Fill(ds, "ds");
                    cmd.Parameters.Clear();
                    LogSql(sql);
                }
                catch (MySqlException ex)
                {
                    throw new Exception(ex.Message);
                }
                finally
                {
                    if (cmd != null)
                    {
                        cmd.Dispose();
                    }
                }

                return ds;
            }
        }

        private static void PrepareCommand(MySqlCommand cmd, MySqlConnection conn, MySqlTransaction trans, string cmdText, MySqlParameter[] cmdParms)
        {
            if (conn.State != ConnectionState.Open)
                conn.Open();
            cmd.Connection = conn;
            cmd.CommandText = cmdText;
            if (trans != null)
                cmd.Transaction = trans;
            cmd.CommandType = CommandType.Text;//cmdType;
            if (cmdParms != null)
            {


                foreach (MySqlParameter parameter in cmdParms)
                {
                    if ((parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.Input) &&
                        (parameter.Value == null))
                    {
                        parameter.Value = DBNull.Value;
                    }
                    cmd.Parameters.Add(parameter);
                }
            }
        }
    }

}
