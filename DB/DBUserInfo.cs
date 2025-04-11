using MySql.Data.MySqlClient;
using Server.Tools;
using Server.Data;

namespace GameDBServer.DB
{
    /// <summary>
    /// Thông tin tài khoản
    /// </summary>
    public class DBUserInfo
    {
        #region Cơ bản
        /// <summary>
        /// ID tài khoản
        /// </summary>
        public string? UserID
        {
            get;
            set;
        }

        private List<int> _ListRoleIDs = new List<int>();
        /// <summary>
        /// Danh sách ID nhân vật
        /// </summary>
        public List<int> ListRoleIDs
        {
            get { return _ListRoleIDs; }
        }

        private List<int> _ListRoleSexes = new List<int>();
        /// <summary>
        /// Danh sách giới tính
        /// </summary>
        public List<int> ListRoleSexes
        {
            get { return _ListRoleSexes; }
        }

        private List<int> _ListRoleOccups = new List<int>();
        /// <summary>
        /// Danh sách môn phái
        /// </summary>
        public List<int> ListRoleOccups
        {
            get { return _ListRoleOccups; }
        }

        private List<string> _ListRoleNames = new List<string>();
        /// <summary>
        /// Danh sách tên
        /// </summary>
        public List<string> ListRoleNames
        {
            get { return _ListRoleNames; }
        }

        private List<int> _ListRoleLevels = new List<int>();
        /// <summary>
        /// Danh sách cấp độ
        /// </summary>
        public List<int> ListRoleLevels
        {
            get { return _ListRoleLevels; }
        }

        private List<int> _ListRoleZoneIDs = new List<int>();
        /// <summary>
        /// Danh sách máy chủ
        /// </summary>
        public List<int> ListRoleZoneIDs
        {
            get { return _ListRoleZoneIDs; }
        }

        /// <summary>
        /// Đồng
        /// </summary>
        public int Money
        {
            get;
            set;
        }

        /// <summary>
        /// Tổng số đồng đã nạp
        /// </summary>
        public int RealMoney
        {
            get;
            set;
        }

        /// <summary>
        /// Nội dung gì đó khi Logout
        /// </summary>
        public string PushMessageID
        {
            get;
            set;
        }
        #endregion

        #region Mở rộng

        private long _LastReferenceTicks = DateTime.Now.Ticks / 10000;
        /// <summary>
        /// Thời gian sử dụng lần cuối
        /// </summary>
        public long LastReferenceTicks
        {
            get { return _LastReferenceTicks; }
            set { _LastReferenceTicks = value; }
        }

        /// <summary>
        /// Thời gian đăng xuất khỏi GS
        /// </summary>
        public long LogoutServerTicks = 0;

        #endregion

        #region Truy vấn DB

        /// <summary>
        /// Truy vấn dữ liệu thông tin tài khoản
        /// </summary>
        public bool Query(MySqlConnection conn, string userID)
        {
            LogManager.WriteLog(LogTypes.Info, $"Query role info from DB: {userID}");
            this.UserID = userID;

            // Query t_roles
            string queryRoles = @"
            SELECT rid, userid, rname, sex, occupation, level, zoneid
            FROM t_roles
            WHERE userid = @userID AND isdel = 0
            ORDER BY level DESC
            LIMIT 4";

            using (var cmd = new MySqlCommand(queryRoles, conn))
            {
                cmd.Parameters.AddWithValue("@userID", userID);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ListRoleIDs.Add(reader.GetInt32("rid"));
                        ListRoleNames.Add(reader.GetString("rname"));
                        ListRoleSexes.Add(reader.GetInt32("sex"));
                        ListRoleOccups.Add(reader.GetInt32("occupation"));
                        ListRoleLevels.Add(reader.GetInt32("level"));
                        ListRoleZoneIDs.Add(reader.GetInt32("zoneid"));
                    }
                }
            }

            // Query t_money
            this.Money = 0;
            string queryMoney = "SELECT money, realmoney FROM t_money WHERE userid = @userID";
            using (var cmd = new MySqlCommand(queryMoney, conn))
            {
                cmd.Parameters.AddWithValue("@userID", userID);
                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        this.Money = reader.GetInt32("money");
                        this.RealMoney = reader.GetInt32("realmoney");
                    }
                }
            }

            // Query t_pushmessageinfo
            string queryPush = "SELECT pushid FROM t_pushmessageinfo WHERE userid = @userID";
            using (var cmd = new MySqlCommand(queryPush, conn))
            {
                cmd.Parameters.AddWithValue("@userID", userID);
                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        this.PushMessageID = reader.GetString("pushid");
                    }
                }
            }

            return true;
        }

        #endregion

        #region Truy vấn thông tin

        /// <summary>
        /// Trả về thông tin tài khoản thu gọn
        /// </summary>
        /// <param name="userId"></param>
        /// <param name="roleId"></param>
        /// <param name="OnlyZoneId"></param>
        /// <returns></returns>
        public UserMiniData GetUserMiniData(string userId, int roleId, int OnlyZoneId)
        {
            UserMiniData userMiniData = new UserMiniData();
            userMiniData.UserId = this.UserID;
            userMiniData.RealMoney = this.RealMoney;

            string query = @"
        SELECT rid, level, regtime, lasttime, logofftime
        FROM t_roles
        WHERE userid = @userId AND isdel = 0 AND zoneid = @zoneId";
            using (var conn = new MyDbConnection3())
            {
                using (var cmd = new MySqlCommand(query, conn.DbConn))
                {
                    cmd.Parameters.AddWithValue("@userId", userId);
                    cmd.Parameters.AddWithValue("@zoneId", OnlyZoneId);

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int rid = reader.GetInt32("rid");
                            int level = reader.GetInt32("level");

                            DateTime createTime = reader["regtime"] != DBNull.Value ? Convert.ToDateTime(reader["regtime"]) : DateTime.MinValue;
                            DateTime lastTime = reader["lasttime"] != DBNull.Value ? Convert.ToDateTime(reader["lasttime"]) : DateTime.MinValue;
                            DateTime logoffTime = reader["logofftime"] != DBNull.Value ? Convert.ToDateTime(reader["logofftime"]) : DateTime.MinValue;

                            if (rid == roleId)
                            {
                                userMiniData.RoleCreateTime = createTime;
                                userMiniData.RoleLastLoginTime = lastTime;
                                userMiniData.RoleLastLogoutTime = logoffTime;
                            }

                            if (userMiniData.MinCreateRoleTime > createTime || userMiniData.MinCreateRoleTime == default)
                            {
                                userMiniData.MinCreateRoleTime = createTime;
                            }

                            if (userMiniData.LastLoginTime < lastTime)
                            {
                                userMiniData.LastLoginTime = lastTime;
                                userMiniData.LastRoleId = rid;
                            }

                            if (userMiniData.LastLogoutTime < logoffTime)
                            {
                                userMiniData.LastLogoutTime = logoffTime;
                            }

                            if (userMiniData.MaxLevel < level)
                            {
                                userMiniData.MaxLevel = level;
                            }
                        }
                    }
                }
            }

            return userMiniData;
        }
        #endregion
    }
}
