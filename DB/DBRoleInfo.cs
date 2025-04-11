using GameDBServer.Logic;
using GameDBServer.Logic.Rank;
using MySql.Data.MySqlClient;
using Server.Data;
using Server.Tools;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;

namespace GameDBServer.DB
{
    /// <summary>
    /// Code lại cho đỡ toác
    /// </summary>
    public class DBRoleInfo
    {
        #region Các thực thể

        /// <summary>
        /// RoleiD
        /// </summary>
        public int RoleID
        {
            get;
            set;
        }



        private object _MoneyLock = new object();

        public object GetMoneyLock
        {
            get { return _MoneyLock; }
        }

        /// <summary>
        /// 用户ID
        /// </summary>
        public string UserID
        {
            get;
            set;
        }

        /// <summary>
        /// 角色名称
        /// </summary>
        public string RoleName
        {
            get;
            set;
        }

        /// <summary>
        /// 角色性别
        /// </summary>
        public int RoleSex
        {
            get;
            set;
        }

        /// <summary>
        /// ID phái
        /// </summary>
        public int Occupation
        {
            get;
            set;
        }

        /// <summary>
        /// ID nhánh
        /// </summary>
        public int SubID
        {
            get;
            set;
        }

        /// <summary>
        /// 角色级别
        /// </summary>
        public int Level
        {
            get;
            set;
        }

        /// <summary>
        /// 角色头像
        /// </summary>
        public int RolePic
        {
            get;
            set;
        }

        /// <summary>
        /// 角色帮派
        /// </summary>
        public int GuildID
        {
            get;
            set;
        }

        /// <summary>
        /// 角色的绑定钱币
        /// </summary>
        public int Money1
        {
            get;
            set;
        }

        /// <summary>
        /// 角色的非绑定钱币
        /// </summary>
        public int Money2
        {
            get;
            set;
        }

        /// <summary>
        /// 角色的当前经验
        /// </summary>
        public long Experience
        {
            get;
            set;
        }

        /// <summary>
        /// 角色的PK模式
        /// </summary>
        public int PKMode
        {
            get;
            set;
        }

        /// <summary>
        /// 角色的PK值
        /// </summary>
        public int PKValue
        {
            get;
            set;
        }

        /// <summary>
        /// 角色的位置
        /// </summary>
        public string Position
        {
            get;
            set;
        }

        /// <summary>
        /// 注册时间
        /// </summary>
        public string RegTime
        {
            get;
            set;
        }

        /// <summary>
        /// 最后登录时间
        /// </summary>
        public long LastTime
        {
            get;
            set;
        }

        /// <summary>
        /// 当前背包的页数(总个数 - 1)
        /// </summary>
        public int BagNum
        {
            get;
            set;
        }

        /// <summary>
        /// 主快捷面板的映射
        /// </summary>
        public string MainQuickBarKeys
        {
            get;
            set;
        }

        /// <summary>
        /// 辅助快捷面板的映射
        /// </summary>
        public string OtherQuickBarKeys
        {
            get;
            set;
        }

        /// <summary>
        /// 登录的次数
        /// </summary>
        public int LoginNum
        {
            get;
            set;
        }

        /// <summary>
        /// 剩余的自动挂机时间
        /// </summary>
        public int LeftFightSeconds
        {
            get;
            set;
        }

        /// <summary>
        /// 线路ID
        /// </summary>
        public int ServerLineID
        {
            get;
            set;
        }

        /// <summary>
        /// 总的在线秒数
        /// </summary>
        public int TotalOnlineSecs
        {
            get;
            set;
        }

        /// <summary>
        /// 防止沉迷在线秒数
        /// </summary>
        public int AntiAddictionSecs
        {
            get;
            set;
        }

        /// <summary>
        /// 上次离线时间
        /// </summary>
        public long LogOffTime
        {
            get;
            set;
        }

        /// <summary>
        /// 系统绑定的银两
        /// </summary>
        public int YinLiang
        {
            get;
            set;
        }

        /// <summary>
        /// 已经完成的主线任务的ID
        /// </summary>
        public int MainTaskID
        {
            get;
            set;
        }

        /// <summary>
        /// 当前的PK点
        /// </summary>
        public int PKPoint
        {
            get;
            set;
        }

        /// <summary>
        /// ID gia tộc
        /// </summary>
        public int FamilyID { get; set; }

        /// <summary>
        /// Tên gia tộc
        /// </summary>
        public string? FamilyName { get; set; }

        /// <summary>
        /// Chức vị trong gia tộc
        /// </summary>
        public int FamilyRank { get; set; }

        /// <summary>
        /// Uy danh
        /// </summary>
        public int Prestige { get; set; }

        /// <summary>
        /// 杀BOSS的总个数
        /// </summary>
        public int KillBoss
        {
            get;
            set;
        }

        /// <summary>
        /// 充值TaskID
        /// </summary>
        public int CZTaskID
        {
            get;
            set;
        }

        /// <summary>
        /// 登录日ID
        /// </summary>
        public int LoginDayID
        {
            get;
            set;
        }

        /// <summary>
        /// 登录日次数
        /// </summary>
        public int LoginDayNum
        {
            get;
            set;
        }

        /// <summary>
        /// 区ID
        /// </summary>
        public int ZoneID
        {
            get;
            set;
        }

        /// <summary>
        /// 帮会名称
        /// </summary>
        public string? GuildName
        {
            get;
            set;
        }

        /// <summary>
        /// 帮会职务
        /// </summary>
        public int GuildRank
        {
            get;
            set;
        }


        public int RoleGuildMoney
        {
            get;
            set;
        }

        /// <summary>
        /// 用户名称
        /// </summary>
        public string UserName
        {
            get;
            set;
        }

        /// <summary>
        /// 上次的mailID
        /// </summary>
        public int LastMailID
        {
            get;
            set;
        }

        /// <summary>
        /// 单次奖励记录标志位
        /// </summary>
        public long OnceAwardFlag
        {
            get;
            set;
        }

        /// <summary>
        /// 系统绑定的金币
        /// </summary>
        public int Gold
        {
            get;
            set;
        }

        /// <summary>
        /// 永久禁止聊天
        /// </summary>
        public int BanChat
        {
            get;
            set;
        }

        /// <summary>
        /// 永久禁止登陆
        /// </summary>
        public int BanLogin
        {
            get;
            set;
        }

        // MU项目增加字段 [11/30/2013 LiaoWei]
        /// <summary>
        /// 新人标记
        /// </summary>
        public int IsFlashPlayer
        {
            get;
            set;
        }

        // MU项目增加字段 [12/10/2013 LiaoWei]
        /// <summary>
        /// 被崇拜计数
        /// </summary>
        public int AdmiredCount
        {
            get;
            set;
        }

        // MU项目增加字段 [4/23/2014 LiaoWei]
        /// <summary>
        /// 消息推送ID
        /// </summary>
        public string PushMsgID
        {
            get;
            set;
        }

        // MU项目增加字段 [8/21/2014 LiaoWei]
        /// <summary>
        /// vip奖励领取标记
        /// </summary>
        public int VipAwardFlag
        {
            get;
            set;
        }

        /// <summary>
        /// VIP等级
        /// </summary>
        public int VIPLevel
        {
            get;
            set;
        }

        /// <summary>
        /// 角色的当前金币
        /// </summary>
        public long store_yinliang
        {
            get;
            set;
        }

        /// <summary>
        /// 角色的当前绑定金币
        /// </summary>
        public int store_money
        {
            get;
            set;
        }

        /// <summary>

        // 玩家充值/消费数值缓存
        private UserRankValueCache rankValue = new UserRankValueCache();

        /// <summary>
        /// 玩家领取奖励的状态
        /// </summary>
        public UserRankValueCache RankValue
        {
            get { return rankValue; }
            set { rankValue = value; }
        }

        #endregion 基本数据

        #region 扩展数据

        /// <summary>
        /// 角色参数表
        /// </summary>
        public ConcurrentDictionary<string, RoleParamsData> RoleParamsDict { get; set; } = new ConcurrentDictionary<string, RoleParamsData>();

        /// <summary>
        /// Danh sách nhiệm vụ đã hoàn thành
        /// </summary>
        public List<OldTaskData> OldTasks
        {
            get;
            set;
        }

        /// <summary>
        /// Danh sách nhiệm vụ
        /// </summary>
        public ConcurrentDictionary<int, TaskData> DoingTaskList
        {
            get;
            set;
        }

        /// <summary>
        /// Danh sách vật phẩm
        /// </summary>
        public ConcurrentDictionary<int, GoodsData> GoodsDataList
        {
            get;
            set;
        }

        /// <summary>
        /// 已经使用的物品限制列表
        /// </summary>
        public List<GoodsLimitData> GoodsLimitDataList
        {
            get;
            set;
        }

        /// <summary>
        /// Danh sách bạn bè
        /// </summary>
        public List<FriendData> FriendDataList
        {
            get;
            set;
        }

        /// <summary>
        /// Danh sách kỹ năng
        /// </summary>
        public ConcurrentDictionary<int, SkillData> SkillDataList
        {
            get;
            set;
        }

        /// <summary>
        /// Danh sách Buff
        /// </summary>
        public ConcurrentDictionary<int, BufferData> BufferDataList
        {
            get;
            set;
        }

        /// <summary>
        /// 跑环任务的数据
        /// </summary>
        public List<DailyTaskData> MyDailyTaskDataList
        {
            get;
            set;
        }

        /// <summary>
        /// 随身仓库数据
        /// </summary>
        public PortableBagData MyPortableBagData
        {
            get;
            set;
        }

        /// <summary>
        /// 活动送礼相关数据是否已经存在？
        /// </summary>
        public bool ExistsMyHuodongData
        {
            get;
            set;
        }

        /// <summary>
        /// 活动送礼相关数据
        /// </summary>
        public HuodongData MyHuodongData
        {
            get;
            set;
        }

        /// <summary>
        /// [bing] 结婚数据
        /// </summary>
        public MarriageData MyMarriageData
        {
            get;
            set;
        }

        public Dictionary<int, int> MyMarryPartyJoinList
        {
            get;
            set;
        }



        /// <summary>
        /// 角色每日数据
        /// </summary>
        public RoleDailyData MyRoleDailyData
        {
            get;
            set;
        }


        /// <summary>
        /// 上次使用访问的时间
        /// </summary>
        private long _LastReferenceTicks = DateTime.Now.Ticks / 10000;

        /// <summary>
        /// 上次使用访问的时间
        /// </summary>
        public long LastReferenceTicks
        {
            get { return _LastReferenceTicks; }
            set { _LastReferenceTicks = value; }
        }




        /// <summary>
        /// 最后一次登陆的IP
        /// </summary>
        public string LastIP
        {
            get;
            set;
        }

        public List<int> GroupMailRecordList
        {
            get;
            set;
        }

        /// <summary>
        ///  七日活动
        /// </summary>
        public Dictionary<int, Dictionary<int, SevenDayItemData>> SevenDayActDict
        {
            get;
            set;
        }

        /// <summary>
        /// 封停交易
        /// </summary>
        public long BanTradeToTicks
        {
            get;
            set;
        }

        /// <summary>
        /// 专享活动数据
        /// </summary>
        public Dictionary<int, SpecActInfoDB> SpecActInfoDict
        {
            get;
            set;
        }

        #endregion 扩展数据

        #region 从数据库查询信息

        public static void DBTableRow2RoleInfo(DBRoleInfo dbRoleInfo, MySqlDataReader reader)
        {
            dbRoleInfo.RoleID = reader.GetInt32("rid");
            dbRoleInfo.UserID = reader.GetString("userid");
            dbRoleInfo.RoleName = reader.GetString("rname");
            dbRoleInfo.RoleSex = reader.GetInt32("sex");
            dbRoleInfo.Occupation = reader.GetInt32("occupation");
            dbRoleInfo.SubID = reader.GetInt32("sub_id");
            dbRoleInfo.Level = reader.GetInt32("level");
            dbRoleInfo.RolePic = reader.GetInt32("pic");

            dbRoleInfo.Money1 = reader.GetInt32("money1");
            dbRoleInfo.Money2 = reader.GetInt32("money2");
            dbRoleInfo.Experience = reader.GetInt64("experience");
            dbRoleInfo.PKMode = reader.GetInt32("pkmode");
            dbRoleInfo.PKValue = reader.GetInt32("pkvalue");
            dbRoleInfo.Position = reader.GetString("position");
            dbRoleInfo.RegTime = reader.GetDateTime("regtime").ToString("yyyy-MM-dd HH:mm:ss");
            dbRoleInfo.LastTime = DataHelper.ConvertToTicks(reader.GetDateTime("lasttime").ToString("yyyy-MM-dd HH:mm:ss"));
            dbRoleInfo.BagNum = reader.GetInt32("bagnum");

            dbRoleInfo.MainQuickBarKeys = reader.GetString("main_quick_keys");
            dbRoleInfo.OtherQuickBarKeys = reader.GetString("other_quick_keys");
            dbRoleInfo.LoginNum = reader.GetInt32("loginnum");
            dbRoleInfo.LeftFightSeconds = reader.GetInt32("leftfightsecs");
            dbRoleInfo.TotalOnlineSecs = reader.GetInt32("totalonlinesecs");
            dbRoleInfo.AntiAddictionSecs = reader.GetInt32("antiaddictionsecs");
            dbRoleInfo.LogOffTime = DataHelper.ConvertToTicks(reader.GetDateTime("logofftime").ToString("yyyy-MM-dd HH:mm:ss"));

            dbRoleInfo.YinLiang = reader.GetInt32("yinliang");
            dbRoleInfo.MainTaskID = reader.GetInt32("maintaskid");
            dbRoleInfo.PKPoint = reader.GetInt32("pkpoint");

            dbRoleInfo.KillBoss = reader.GetInt32("killboss");
            dbRoleInfo.CZTaskID = reader.GetInt32("cztaskid");

            dbRoleInfo.LoginDayID = reader.GetInt32("logindayid");
            dbRoleInfo.LoginDayNum = reader.GetInt32("logindaynum");
            dbRoleInfo.ZoneID = reader.GetInt32("zoneid");

            dbRoleInfo.UserName = reader.GetString("username");
            dbRoleInfo.LastMailID = reader.GetInt32("lastmailid");
            dbRoleInfo.OnceAwardFlag = reader.GetInt64("onceawardflag");
            dbRoleInfo.Gold = reader.GetInt32("money2");
            dbRoleInfo.BanChat = reader.GetInt32("banchat");
            dbRoleInfo.BanLogin = reader.GetInt32("banlogin");
            dbRoleInfo.IsFlashPlayer = reader.GetInt32("isflashplayer");

            dbRoleInfo.AdmiredCount = reader.GetInt32("admiredcount");
            dbRoleInfo.store_yinliang = reader.GetInt64("store_yinliang");
            dbRoleInfo.store_money = reader.GetInt32("store_money");
            dbRoleInfo.BanTradeToTicks = reader.GetInt64("ban_trade_to_ticks");

            dbRoleInfo.FamilyID = reader.GetInt32("familyid");
            dbRoleInfo.FamilyName = GetNullableString(reader, "familyname");
            dbRoleInfo.FamilyRank = reader.GetInt32("familyrank");
            dbRoleInfo.Prestige = reader.GetInt32("roleprestige");

            dbRoleInfo.RoleGuildMoney = reader.GetInt32("guildmoney");
            dbRoleInfo.GuildRank = reader.GetInt32("guildrank");
            dbRoleInfo.GuildID = reader.GetInt32("guildid");
            dbRoleInfo.GuildName = GetNullableString(reader, "guildname");

        }
        static string? GetNullableString(MySqlDataReader reader, string columnName)
        {
            int idx = reader.GetOrdinal(columnName);
            return reader.IsDBNull(idx) ? null : reader.GetString(idx);
        }
        public static void DBTableRow2RoleInfo_Params(DBRoleInfo dbRoleInfo, MySqlConnection conn, string tableName, int roleID, bool normalOnly)
        {
            string query = "SELECT pname, pvalue FROM " + tableName + " WHERE rid = @rid";

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleID);

                using (var reader = cmd.ExecuteReader())
                {
                    var dict = dbRoleInfo.RoleParamsDict;

                    while (reader.Read())
                    {
                        string pname = reader["pname"].ToString();
                        string pvalue = reader["pvalue"].ToString();

                        RoleParamsData roleParamsData = new RoleParamsData
                        {
                            ParamName = pname,
                            ParamValue = pvalue,
                            ParamType = RoleParamNameInfo.GetRoleParamType(pname, pvalue)
                        };

                        if (roleParamsData.ParamType.Type > 0 && normalOnly)
                            continue;

                        dict[pname] = roleParamsData;
                    }
                }
            }
        }
        /// <summary>
        /// Truy vấn tên nhân vật theo ID
        /// </summary>
        /// <param name="dbManager"></param>
        /// <param name="roleID"></param>
        public static string QueryRoleNameByRoleID(DBManager dbManager, int roleID)
        {
            /// Tên nhân vật tương ứng
            string roleName = "";

            /// Thông tin nhân vật được cache
            DBRoleInfo roleInfo = dbManager.GetDBRoleInfo(roleID);
            /// Nếu đã được Cache
            if (roleInfo != null)
            {
                roleName = roleInfo.RoleName;
            }
            /// Nếu chưa được Cache
            else
            {
                MySqlConnection? conn = null;

                try
                {
                    conn = dbManager.DBConns.PopDBConnection();
                    string queryString = string.Format("SELECT rname FROM t_roles WHERE rid = {0}", roleID);

                    MySqlCommand? cmd = new MySqlCommand(queryString, conn);
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            roleName = reader["rname"].ToString() ?? "";
                        }
                    }

                    cmd.Dispose();
                    cmd = null;
                }
                catch (Exception ex)
                {
                    LogManager.WriteLog(LogTypes.TeamBattle, ex.ToString());
                }
                finally
                {
                    if (null != conn)
                    {
                        dbManager.DBConns.PushDBConnection(conn);
                    }
                }
            }

            /// Trả về kết quả
            return roleName;
        }

        public static void DBTableRow2RoleInfo_ParamsEx(DBRoleInfo dbRoleInfo, int roleId)
        {
            using (MyDbConnection3 conn = new MyDbConnection3())
            {
                string cmdText = string.Format("select * from t_roleparams_long where rid={0};", roleId);
                using (var reader = conn.ExecuteReader(cmdText))
                {
                    if (reader.HasRows)  // Kiểm tra xem có dữ liệu không
                    {
                        ConcurrentDictionary<string, RoleParamsData> dict = dbRoleInfo.RoleParamsDict;

                        while (reader.Read())  // Duyệt qua từng dòng dữ liệu
                        {
                            int idx = reader.GetInt32(reader.GetOrdinal("idx"));  // Lấy giá trị cột 'idx'
                            int columnCount = reader.FieldCount;  // Số lượng cột trong dòng

                            for (int columnIndex = 2; columnIndex < columnCount; columnIndex++)  // Bắt đầu từ cột thứ 3
                            {
                                RoleParamType roleParamType = RoleParamNameInfo.GetRoleParamType(idx, columnIndex - 2);
                                if (roleParamType != null)
                                {
                                    RoleParamsData roleParamsData = new RoleParamsData()
                                    {
                                        ParamName = roleParamType.ParamName,
                                        ParamValue = reader.GetInt64(columnIndex).ToString(),  // Lấy giá trị cột
                                        ParamType = roleParamType,
                                    };

                                    dict[roleParamsData.ParamName] = roleParamsData;
                                }
                            }
                        }
                    }
                }
                // cmdText = string.Format("select * from t_roleparams_long where rid={0};", roleId);
                // using (var reader = conn.ExecuteReader(cmdText))
                // {
                //     DataTable dataTable = reader.GetSchemaTable();
                //     if (dataTable.Rows.Count > 0)
                //     {
                //         ConcurrentDictionary<string, RoleParamsData> dict = dbRoleInfo.RoleParamsDict;
                //         for (int i = 0; i < dataTable.Rows.Count; i++)
                //         {
                //             DataRow dataRow = dataTable.Rows[i];
                //             int idx = Convert.ToInt32(dataRow["idx"].ToString());
                //             int columnCount = dataRow.ItemArray.Length;
                //             for (int columnIndex = 2; columnIndex < columnCount; columnIndex++)
                //             {
                //                 RoleParamType roleParamType = RoleParamNameInfo.GetRoleParamType(idx, columnIndex - 2);
                //                 if (null != roleParamType)
                //                 {
                //                     RoleParamsData roleParamsData = new RoleParamsData()
                //                     {
                //                         ParamName = roleParamType.ParamName,
                //                         ParamValue = dataRow[columnIndex].ToString(),
                //                         ParamType = roleParamType,
                //                     };
                //
                //                     dict[roleParamsData.ParamName] = roleParamsData;
                //                 }
                //             }
                //         }
                //     }
                //
                // }

                cmdText = string.Format("select * from t_roleparams_char where rid={0};", roleId);
                // using (var reader = conn.ExecuteReader(cmdText))
                // {
                //     DataTable dataTable = reader.GetSchemaTable();
                //     if (dataTable.Rows.Count > 0)
                //     {
                //         ConcurrentDictionary<string, RoleParamsData> dict = dbRoleInfo.RoleParamsDict;
                //         for (int i = 0; i < dataTable.Rows.Count; i++)
                //         {
                //             DataRow dataRow = dataTable.Rows[i];
                //             int idx = Convert.ToInt32(dataRow["idx"].ToString());
                //             int columnCount = dataRow.ItemArray.Length;
                //             for (int columnIndex = 2; columnIndex < columnCount; columnIndex++)
                //             {
                //                 RoleParamType roleParamType = RoleParamNameInfo.GetRoleParamType(idx, columnIndex - 2);
                //                 if (null != roleParamType)
                //                 {
                //                     RoleParamsData roleParamsData = new RoleParamsData()
                //                     {
                //                         ParamName = roleParamType.ParamName,
                //                         ParamValue = dataRow[columnIndex].ToString(),
                //                         ParamType = roleParamType,
                //                     };
                //
                //                     dict[roleParamsData.ParamName] = roleParamsData;
                //                 }
                //             }
                //         }
                //     }
                // }

                using (var reader = conn.ExecuteReader(cmdText))
                {
                    if (reader.HasRows)  // Kiểm tra xem có dữ liệu không
                    {
                        ConcurrentDictionary<string, RoleParamsData> dict = dbRoleInfo.RoleParamsDict;

                        while (reader.Read())  // Duyệt qua từng dòng dữ liệu
                        {
                            int idx = reader.GetInt32(reader.GetOrdinal("idx"));  // Lấy giá trị cột 'idx'
                            int columnCount = reader.FieldCount;  // Số lượng cột trong dòng

                            for (int columnIndex = 2; columnIndex < columnCount; columnIndex++)  // Bắt đầu từ cột thứ 3
                            {
                                RoleParamType roleParamType = RoleParamNameInfo.GetRoleParamType(idx, columnIndex - 2);
                                if (roleParamType != null)
                                {
                                    RoleParamsData roleParamsData = new RoleParamsData()
                                    {
                                        ParamName = roleParamType.ParamName,
                                        ParamValue = reader.GetString(columnIndex),  // Lấy giá trị cột
                                        ParamType = roleParamType,
                                    };

                                    dict[roleParamsData.ParamName] = roleParamsData;
                                }
                            }
                        }
                    }
                }
            }
        }


        public static void DBTableRow2RoleInfo_OldTasks(DBRoleInfo dbRoleInfo, MySqlConnection conn, int roleId)
        {
            string query = "SELECT rid, taskid, count, taskclass FROM t_taskslog WHERE rid = @rid";

            // cmd = new MySQLSelectCommand(conn,
            //      new string[] { "rid", "taskid", "count", "taskclass" },
            //      new string[] { "t_taskslog" }, new object[,] { { "rid", "=", roleID } }, null, null);
            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleId);

                using (var reader = cmd.ExecuteReader())
                {
                    List<OldTaskData> oldTasks = new List<OldTaskData>();

                    while (reader.Read())
                    {
                        string taskClassStr = reader["taskclass"].ToString();
                        int taskClass = 0;

                        if (!string.IsNullOrEmpty(taskClassStr))
                        {
                            taskClass = Convert.ToInt32(taskClassStr);
                        }

                        OldTaskData task = new OldTaskData
                        {
                            TaskID = Convert.ToInt32(reader["taskid"]),
                            DoCount = Convert.ToInt32(reader["count"]),
                            TaskClass = taskClass
                        };

                        oldTasks.Add(task);
                    }

                    dbRoleInfo.OldTasks = oldTasks;
                }
            }
        }

        public static void DBTableRow2RoleInfo_DoingTasks(DBRoleInfo dbRoleInfo, MySqlConnection conn, int roleId)
        {
            // cmd = new MySQLSelectCommand(conn,
            //      new string[] { "Id", "rid", "taskid", "focus", "value1", "value2", "addtime", "starlevel" },
            //      new string[] { "t_tasks" }, new object[,] { { "rid", "=", roleID }, { "isdel", "=", 0 } }, null, null);
            string query = "SELECT id, taskid, value1, value2, focus, addtime, starlevel FROM t_tasks WHERE rid = @rid and isdel = 0";

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleId);

                using (var reader = cmd.ExecuteReader())
                {
                    dbRoleInfo.DoingTaskList = new ConcurrentDictionary<int, TaskData>();

                    while (reader.Read())
                    {
                        TaskData task = new TaskData
                        {
                            DbID = Convert.ToInt32(reader["id"]),
                            DoingTaskID = Convert.ToInt32(reader["taskid"]),
                            DoingTaskVal1 = Convert.ToInt32(reader["value1"]),
                            DoingTaskVal2 = Convert.ToInt32(reader["value2"]),
                            DoingTaskFocus = Convert.ToInt32(reader["focus"]),
                            AddDateTime = DataHelper.ConvertToTicks(reader["addtime"].ToString()),
                            StarLevel = Convert.ToInt32(reader["starlevel"]),
                        };

                        dbRoleInfo.DoingTaskList[task.DbID] = task;
                    }
                }
            }
        }

        public static void DBTableRow2RoleInfo_Goods(DBRoleInfo dbRoleInfo, MySqlConnection conn, int roleId)
        {
            // cmd = new MySQLSelectCommand(conn,
            //          new string[] { "id", "goodsid", "isusing", "forge_level", "starttime", "endtime", "site", "Props", "gcount", "binding", "bagindex", "strong", "series", "otherpramer" },
            //          new string[] { "t_goods" }, new object[,] { { "rid", "=", roleID }, { "gcount", ">", 0 } }, null, new string[,] { { "id", "asc" } });
            string query = "SELECT Id, goodsid, isusing, forge_level, starttime, endtime, site, Props, gcount, binding, bagindex, strong, series, otherpramer " +
                           "FROM t_goods WHERE rid = @rid and gcount > 0 order by id asc";

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleId);

                using (var reader = cmd.ExecuteReader())
                {
                    dbRoleInfo.GoodsDataList = new ConcurrentDictionary<int, GoodsData>();

                    while (reader.Read())
                    {
                        string otherParameter = reader["otherpramer"].ToString();

                        Dictionary<ItemPramenter, string> otherParams = null;

                        if (!string.IsNullOrEmpty(otherParameter))
                        {
                            try
                            {
                                byte[] base64Decoded = Convert.FromBase64String(otherParameter);
                                otherParams = DataHelper.BytesToObject<Dictionary<ItemPramenter, string>>(base64Decoded, 0, base64Decoded.Length);
                            }
                            catch (Exception ex)
                            {
                                // Logging/debug only
                                Console.WriteLine($"Failed to decode otherpramer: {ex.Message}");
                                otherParams = new Dictionary<ItemPramenter, string>();
                            }
                        }

                        GoodsData goodsData = new GoodsData
                        {
                            Id = Convert.ToInt32(reader["Id"]),
                            GoodsID = Convert.ToInt32(reader["goodsid"]),
                            Using = Convert.ToInt32(reader["isusing"]),
                            Forge_level = Convert.ToInt32(reader["forge_level"]),
                            Starttime = reader["starttime"].ToString(),
                            Endtime = reader["endtime"].ToString(),
                            Site = Convert.ToInt32(reader["site"]),
                            Props = reader["Props"].ToString(),
                            GCount = Convert.ToInt32(reader["gcount"]),
                            Binding = Convert.ToInt32(reader["binding"]),
                            BagIndex = Convert.ToInt32(reader["bagindex"]),
                            Strong = Convert.ToInt32(reader["strong"]),
                            Series = Convert.ToInt32(reader["series"]),
                            OtherParams = otherParams,
                        };

                        dbRoleInfo.GoodsDataList[goodsData.Id] = goodsData;
                    }
                }
            }
        }

        /*
        /// <summary>
        /// 将数据库中获取的数据转换为角色数据_好友数据
        /// </summary>
        /// <param name="dbRoleInfo"></param>
        /// <param name="cmd"></param>
        /// <param name="index"></param>
        public static void DBTableRow2RoleInfo_Friends(DBRoleInfo dbRoleInfo, MySQLSelectCommand cmd)
        {
            if (cmd.Table.Rows.Count > 0)
            {
                dbRoleInfo.FriendDataList = new List<FriendData>();
                for (int i = 0; i < cmd.Table.Rows.Count; i++)
                {
                    dbRoleInfo.FriendDataList.Add(new FriendData()
                    {
                        DbID = Convert.ToInt32(cmd.Table.Rows[i]["Id"].ToString()),
                        OtherRoleID = Convert.ToInt32(cmd.Table.Rows[i]["otherid"].ToString()),
                        FriendType = Convert.ToInt32(cmd.Table.Rows[i]["friendType"].ToString()),
                        Relationship = Convert.ToInt32(cmd.Table.Rows[i]["relationship"].ToString()),
                    });
                }
            }
        }
        */

        public static void DBTableRow2RoleInfo_Skills(DBRoleInfo dbRoleInfo, MySqlConnection conn, int roleId)
        {
            // Giả định bảng chứa thông tin kỹ năng là t_skills.
            // cmd = new MySQLSelectCommand(conn,
            //          new string[] { "Id", "skillid", "skilllevel", "lastusedtick", "cooldowntick", "exp" },
            //          new string[] { "t_skills" }, new object[,] { { "rid", "=", roleID } }, null, null);
            string query = @"
        SELECT Id, skillid, skilllevel, lastusedtick, cooldowntick, exp 
        FROM t_skills 
        WHERE rid = @rid";

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleId);

                using (var reader = cmd.ExecuteReader())
                {
                    dbRoleInfo.SkillDataList = new ConcurrentDictionary<int, SkillData>();

                    while (reader.Read())
                    {
                        SkillData skillData = new SkillData
                        {
                            DbID = Convert.ToInt32(reader["Id"]),
                            SkillID = Convert.ToInt32(reader["skillid"]),
                            SkillLevel = Convert.ToInt32(reader["skilllevel"]),
                            LastUsedTick = Convert.ToInt64(reader["lastusedtick"]),
                            Cooldown = Convert.ToInt32(reader["cooldowntick"]),
                            Exp = Convert.ToInt32(reader["exp"])
                        };

                        // Gán vào dictionary với SkillID làm key.
                        dbRoleInfo.SkillDataList[skillData.SkillID] = skillData;
                    }
                }
            }
        }

        public static void DBTableRow2RoleInfo_Buffers(DBRoleInfo dbRoleInfo, MySqlConnection conn, int roleId)
        {
            // cmd = new MySQLSelectCommand(conn,
            //          new string[] { "bufferid", "starttime", "buffersecs", "bufferval", "custom_property" },
            //          new string[] { "t_buffer" }, new object[,] { { "rid", "=", roleID } }, null, null);
            string query = @"
        SELECT bufferid, starttime, buffersecs, custom_property, bufferval 
        FROM t_buffer 
        WHERE rid = @rid";

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleId);

                using (var reader = cmd.ExecuteReader())
                {
                    dbRoleInfo.BufferDataList = new ConcurrentDictionary<int, BufferData>();

                    while (reader.Read())
                    {
                        BufferData buff = new BufferData
                        {
                            BufferID = Convert.ToInt32(reader["bufferid"]),
                            StartTime = Convert.ToInt64(reader["starttime"]),
                            BufferSecs = Convert.ToInt64(reader["buffersecs"]),
                            CustomProperty = reader["custom_property"].ToString(),
                            BufferVal = Convert.ToInt64(reader["bufferval"]),
                            BufferType = 0 // hardcoded như trong bản gốc
                        };

                        dbRoleInfo.BufferDataList[buff.BufferID] = buff;
                    }
                }
            }
        }

        public static void DBTableRow2RoleInfo_DailyTasks(DBRoleInfo dbRoleInfo, MySqlConnection conn, int roleId)
        {
            // cmd = new MySQLSelectCommand(conn,
            //          new string[] { "huanid", "rectime", "recnum", "taskClass", "extdayid", "extnum" },
            //          new string[] { "t_dailytasks" }, new object[,] { { "rid", "=", roleID } }, null, null);
            string query = @"
        SELECT huanid, rectime, recnum, taskClass, extdayid, extnum 
        FROM t_dailytasks 
        WHERE rid = @rid";

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleId);

                using (var reader = cmd.ExecuteReader())
                {
                    if (dbRoleInfo.MyDailyTaskDataList == null)
                    {
                        dbRoleInfo.MyDailyTaskDataList = new List<DailyTaskData>();
                    }

                    while (reader.Read())
                    {
                        DailyTaskData dailyTaskData = new DailyTaskData
                        {
                            HuanID = Convert.ToInt32(reader["huanid"]),
                            RecTime = reader["rectime"].ToString(),
                            RecNum = Convert.ToInt32(reader["recnum"]),
                            TaskClass = Convert.ToInt32(reader["taskClass"]),
                            ExtDayID = Convert.ToInt32(reader["extdayid"]),
                            ExtNum = Convert.ToInt32(reader["extnum"])
                        };

                        dbRoleInfo.MyDailyTaskDataList.Add(dailyTaskData);
                    }
                }
            }
        }

        public static void DBTableRow2RoleInfo_PortableBag(DBRoleInfo dbRoleInfo, MySqlConnection conn, int roleId)
        {
            // cmd = new MySQLSelectCommand(conn,
            //          new string[] { "extgridnum" },
            //          new string[] { "t_ptbag" }, new object[,] { { "rid", "=", roleID } }, null, null);

            string query = "SELECT extgridnum FROM t_ptbag WHERE rid = @rid LIMIT 1";

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleId);

                using (var reader = cmd.ExecuteReader())
                {
                    dbRoleInfo.MyPortableBagData = new PortableBagData
                    {
                        GoodsUsedGridNum = 0
                    };

                    if (reader.Read())
                    {
                        dbRoleInfo.MyPortableBagData.ExtGridNum = Convert.ToInt32(reader["extgridnum"]);
                    }
                }
            }
        }

        public static void DBTableRow2RoleInfo_HuodongData(DBRoleInfo dbRoleInfo, MySqlConnection conn, int roleId)
        {
            // cmd = new MySQLSelectCommand(conn,
            //          new string[] { "loginweekid", "logindayid", "loginnum", "newstep", "steptime", "lastmtime", "curmid", "curmtime", "songliid", "logingiftstate", "onlinegiftstate", "lastlimittimehuodongid", "lastlimittimedayid", "limittimeloginnum", "limittimegiftstate", "everydayonlineawardstep", "geteverydayonlineawarddayid", "serieslogingetawardstep", "seriesloginawarddayid", "seriesloginawardgoodsid", "everydayonlineawardgoodsid" },
            //          new string[] { "t_huodong" }, new object[,] { { "rid", "=", roleID } }, null, null);
            const string query = "SELECT * FROM t_huodong WHERE rid = @rid LIMIT 1";

            dbRoleInfo.ExistsMyHuodongData = false;
            dbRoleInfo.MyHuodongData = new HuodongData();

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleId);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        dbRoleInfo.ExistsMyHuodongData = true;

                        dbRoleInfo.MyHuodongData.LastWeekID = reader["loginweekid"].ToString();
                        dbRoleInfo.MyHuodongData.LastDayID = reader["logindayid"].ToString();
                        dbRoleInfo.MyHuodongData.LoginNum = Convert.ToInt32(reader["loginnum"]);
                        dbRoleInfo.MyHuodongData.NewStep = Convert.ToInt32(reader["newstep"]);
                        dbRoleInfo.MyHuodongData.StepTime = DataHelper.ConvertToTicks(reader["steptime"].ToString());
                        dbRoleInfo.MyHuodongData.LastMTime = Convert.ToInt32(reader["lastmtime"]);
                        dbRoleInfo.MyHuodongData.CurMID = reader["curmid"].ToString();
                        dbRoleInfo.MyHuodongData.CurMTime = Convert.ToInt32(reader["curmtime"]);
                        dbRoleInfo.MyHuodongData.SongLiID = Convert.ToInt32(reader["songliid"]);
                        dbRoleInfo.MyHuodongData.LoginGiftState = Convert.ToInt32(reader["logingiftstate"]);
                        dbRoleInfo.MyHuodongData.OnlineGiftState = Convert.ToInt32(reader["onlinegiftstate"]);
                        dbRoleInfo.MyHuodongData.LastLimitTimeHuoDongID = Convert.ToInt32(reader["lastlimittimehuodongid"]);
                        dbRoleInfo.MyHuodongData.LastLimitTimeDayID = Convert.ToInt32(reader["lastlimittimedayid"]);
                        dbRoleInfo.MyHuodongData.LimitTimeLoginNum = Convert.ToInt32(reader["limittimeloginnum"]);
                        dbRoleInfo.MyHuodongData.LimitTimeGiftState = Convert.ToInt32(reader["limittimegiftstate"]);
                        dbRoleInfo.MyHuodongData.EveryDayOnLineAwardStep = Convert.ToInt32(reader["everydayonlineawardstep"]);
                        dbRoleInfo.MyHuodongData.GetEveryDayOnLineAwardDayID = Convert.ToInt32(reader["geteverydayonlineawarddayid"]);
                        dbRoleInfo.MyHuodongData.SeriesLoginGetAwardStep = Convert.ToInt32(reader["serieslogingetawardstep"]);
                        dbRoleInfo.MyHuodongData.SeriesLoginAwardDayID = Convert.ToInt32(reader["seriesloginawarddayid"]);
                        dbRoleInfo.MyHuodongData.SeriesLoginAwardGoodsID = reader["seriesloginawardgoodsid"].ToString();
                        dbRoleInfo.MyHuodongData.EveryDayOnLineAwardGoodsID = reader["everydayonlineawardgoodsid"].ToString();
                    }
                }
            }
        }


        /// <summary>
        /// Truy vấn danh sách bạn bè
        /// </summary>
        /// <param name="dbRoleInfo"></param>
        /// <param name="roleID"></param>
        public static void DBTableRow2RoleInfo_Friends(DBRoleInfo dbRoleInfo, int roleID)
        {
            dbRoleInfo.FriendDataList = new List<FriendData>();
            string str = string.Format("SELECT Id, myid, otherid, relationship, friendType FROM t_friends WHERE myid = {0} OR otherid = {1}", roleID, roleID);
            GameDBManager.SystemServerSQLEvents.AddEvent(string.Format("+SQL: {0}", str), EventLevels.Important);
            MySqlConnection? connection = DBManager.getInstance().DBConns.PopDBConnection();
            try
            {
                MySqlCommand? command = new MySqlCommand(str, connection);
                using (var reader = command.ExecuteReader())
                {

                    while (reader.Read())
                    {
                        int myID = Convert.ToInt32(reader["myid"].ToString());
                        int otherID = Convert.ToInt32(reader["otherid"].ToString());

                        /// Nếu bản thân là myid
                        if (roleID == myID)
                        {
                            dbRoleInfo.FriendDataList.Add(new FriendData()
                            {
                                DbID = Convert.ToInt32(reader["Id"].ToString()),
                                OtherRoleID = otherID,
                                Relationship = Convert.ToInt32(reader["relationship"].ToString()),
                                FriendType = Convert.ToInt32(reader["friendType"].ToString()),
                            });
                        }
                        /// Nếu bản thân là otherid
                        else
                        {
                            dbRoleInfo.FriendDataList.Add(new FriendData()
                            {
                                DbID = Convert.ToInt32(reader["Id"].ToString()),
                                OtherRoleID = myID,
                                Relationship = Convert.ToInt32(reader["relationship"].ToString()),
                                FriendType = Convert.ToInt32(reader["friendType"].ToString()),
                            });
                        }
                    }
                }
                command.Dispose();
                command = null;
            }
            finally
            {
                DBManager.getInstance().DBConns.PushDBConnection(connection);
            }
        }



        public static void DBTableRow2RoleInfo_DailyData(DBRoleInfo dbRoleInfo, MySqlConnection conn, int roleId)
        {

            // cmd = new MySQLSelectCommand(conn,
            //          new string[] { "expdayid", "todayexp", "linglidayid", "todaylingli", "killbossdayid", "todaykillboss", "fubendayid", "todayfubennum", "wuxingdayid", "wuxingnum" },
            //          new string[] { "t_dailydata" }, new object[,] { { "rid", "=", roleID } }, null, null);
            const string query = "SELECT * FROM t_dailydata WHERE rid = @rid LIMIT 1";

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleId);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        dbRoleInfo.MyRoleDailyData = new RoleDailyData()
                        {
                            ExpDayID = Convert.ToInt32(reader["expdayid"]),
                            TodayExp = Convert.ToInt32(reader["todayexp"]),
                            LingLiDayID = Convert.ToInt32(reader["linglidayid"]),
                            TodayLingLi = Convert.ToInt32(reader["todaylingli"]),
                            KillBossDayID = Convert.ToInt32(reader["killbossdayid"]),
                            TodayKillBoss = Convert.ToInt32(reader["todaykillboss"]),
                            FuBenDayID = Convert.ToInt32(reader["fubendayid"]),
                            TodayFuBenNum = Convert.ToInt32(reader["todayfubennum"]),
                            WuXingDayID = Convert.ToInt32(reader["wuxingdayid"]),
                            WuXingNum = Convert.ToInt32(reader["wuxingnum"]),
                        };
                    }
                }
            }
        }








        /// <summary>
        /// 根据角色名查询角色ID
        /// 不要根据这个方法检查角色名是否存在
        /// </summary>
        public static int QueryRoleID_ByRolename(MySqlConnection conn, String strRoleName)
        {
            List<Tuple<int, string>> idList = QueryRoleIdList_ByRolename_IgnoreDbCmp(conn, strRoleName);

            int roleId = -1;
            if (idList != null)
            {
                var tuple = idList.Find(_t => _t.Item2 == strRoleName);
                roleId = tuple != null ? tuple.Item1 : -1;
            }

            return roleId;
        }

        /// <summary>
        /// 查询名字对应的一系列角色id ！！！
        /// 因为现在数据库比较名字未区分大小写，风轻云淡 和 风清云淡 在数据库比较时竟然一样！
        /// 所以查询名字的时候把能查出来的角色id都列出来
        /// </summary>
        public static List<Tuple<int, string>> QueryRoleIdList_ByRolename_IgnoreDbCmp(MySqlConnection conn, string rolename)
        {
            List<Tuple<int, string>> resultList = new List<Tuple<int, string>>();

            string sql = string.Format("SELECT rid,rname FROM t_roles where rname='{0}'", rolename);
            GameDBManager.SystemServerSQLEvents.AddEvent(string.Format("+SQL: {0}", sql), EventLevels.Important);
            MySqlCommand cmd = new MySqlCommand(sql, conn);

            using (MySqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    int oneRoleId = Convert.ToInt32(reader["rid"].ToString());
                    string oneRolename = reader["rname"].ToString();

                    resultList.Add(new Tuple<int, string>(oneRoleId, oneRolename));
                }

            }

            cmd.Dispose();
            cmd = null;

            return resultList;
        }


        public static void DBTableRow2RoleInfo_GMailInfo(DBRoleInfo dbRoleInfo, MySqlConnection conn, int roleId)
        {
            // cmd = new MySQLSelectCommand(conn,
            //          new string[] { "roleid", "gmailid" },
            //          new string[] { "t_rolegmail_record" }, new object[,] { { "roleid", "=", roleID } }, null, null);
            const string query = "SELECT gmailid FROM t_rolegmail_record WHERE roleid = @rid";

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleId);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        dbRoleInfo.GroupMailRecordList = new List<int>();

                        while (reader.Read())
                        {
                            dbRoleInfo.GroupMailRecordList.Add(Convert.ToInt32(reader["gmailid"]));
                        }
                    }
                }
            }
        }


        public static void DBTableRow2RoleInfo_SevenDayActData(DBRoleInfo dbRoleInfo, MySqlConnection conn, int roleId)
        {

            // cmd = new MySQLSelectCommand(conn,
            //          new string[] { "roleid", "act_type", "id", "award_flag", "param1", "param2" },
            //          new string[] { "t_seven_day_act" }, new object[,] { { "roleid", "=", roleID } }, null, null);
            const string query = "SELECT roleid, act_type, id, award_flag, param1, param2 FROM t_seven_day_act WHERE roleid = @roleid";

            dbRoleInfo.SevenDayActDict = new Dictionary<int, Dictionary<int, SevenDayItemData>>();

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@roleid", roleId);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        SevenDayItemData itemData = new SevenDayItemData
                        {
                            AwardFlag = Convert.ToInt32(reader["award_flag"]),
                            Params1 = Convert.ToInt32(reader["param1"]),
                            Params2 = Convert.ToInt32(reader["param2"]),
                        };

                        int actType = Convert.ToInt32(reader["act_type"]);
                        int id = Convert.ToInt32(reader["id"]);

                        if (!dbRoleInfo.SevenDayActDict.TryGetValue(actType, out var itemDict))
                        {
                            itemDict = new Dictionary<int, SevenDayItemData>();
                            dbRoleInfo.SevenDayActDict[actType] = itemDict;
                        }

                        itemDict[id] = itemData;
                    }
                }
            }
        }

        public static void DBTableRow2RoleInfo_SpecialActivityData(DBRoleInfo dbRoleInfo, MySqlConnection conn, int roleId)
        {
            // cmd = new MySQLSelectCommand(conn,
            //          new string[] { "rid", "groupid", "actid", "purchaseNum", "countNum", "active" },
            //          new string[] { "t_special_activity" }, new object[,] { { "rid", "=", roleID } }, null, null);

            const string query = "SELECT groupid, actid, purchaseNum, countNum, active FROM t_special_activity WHERE rid = @roleid";

            dbRoleInfo.SpecActInfoDict = new Dictionary<int, SpecActInfoDB>();

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@roleid", roleId);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        SpecActInfoDB itemData = new SpecActInfoDB
                        {
                            GroupID = Convert.ToInt32(reader["groupid"]),
                            ActID = Convert.ToInt32(reader["actid"]),
                            PurNum = Convert.ToInt32(reader["purchaseNum"]),
                            CountNum = Convert.ToInt32(reader["countNum"]),
                            Active = Convert.ToInt16(reader["active"]),
                        };

                        dbRoleInfo.SpecActInfoDict[itemData.ActID] = itemData;
                    }
                }
            }
        }
        public static bool TryGetRoleByID(MySqlConnection conn, int roleID, bool bUseIsdel, out DBRoleInfo roleInfo)
        {
            roleInfo = null;

            // Các trường cần SELECT
            string[] columns = {
        "rid", "userid", "rname", "sex", "occupation", "sub_id", "level", "pic", "money1", "money2",
        "experience", "pkmode", "pkvalue", "position", "regtime", "lasttime", "bagnum", "main_quick_keys",
        "other_quick_keys", "loginnum", "leftfightsecs", "totalonlinesecs", "antiaddictionsecs", "logofftime",
        "yinliang", "maintaskid", "pkpoint", "killboss", "cztaskid", "logindayid", "logindaynum", "zoneid",
        "guildname", "guildrank", "guildid", "guildmoney", "username", "lastmailid", "onceawardflag", "banchat",
        "banlogin", "isflashplayer", "admiredcount", "store_yinliang", "store_money", "ban_trade_to_ticks",
        "familyid", "familyname", "familyrank", "roleprestige"
    };

            string query = $"SELECT {string.Join(",", columns)} FROM t_roles WHERE rid = @rid";
            if (bUseIsdel)
            {
                query += " AND isdel = 0";
            }
            query += " ORDER BY level DESC LIMIT 4";

            using (var cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleID);

                using (var reader = cmd.ExecuteReader())
                {
                    if (!reader.HasRows)
                        return false;

                    if (reader.Read())
                    {
                        roleInfo = new DBRoleInfo();
                        DBTableRow2RoleInfo(roleInfo, reader); // dùng hàm đã viết trước
                        return true;
                    }
                }
            }

            return false;
        }
        /// <summary>
        /// Truy vấn thông tin nhân vật từ DB
        /// </summary>
        /// <param name="bUseIsdel">Có phải nhân vật chưa bị xóa không</param>
        public bool Query(MySqlConnection conn, int roleID, bool bUseIsdel = true)
        {
            LogManager.WriteLog(LogTypes.Info, string.Format("Load role data from DB: {0}", roleID));

            MySqlCommand? cmd = null;

            string[] columns = {
        "rid", "userid", "rname", "sex", "occupation", "sub_id", "level", "pic", "money1", "money2",
        "experience", "pkmode", "pkvalue", "position", "regtime", "lasttime", "bagnum", "main_quick_keys",
        "other_quick_keys", "loginnum", "leftfightsecs", "totalonlinesecs", "antiaddictionsecs", "logofftime",
        "yinliang", "maintaskid", "pkpoint", "killboss", "cztaskid", "logindayid", "logindaynum", "zoneid",
        "guildname", "guildrank", "guildid", "guildmoney", "username", "lastmailid", "onceawardflag", "banchat",
        "banlogin", "isflashplayer", "admiredcount", "store_yinliang", "store_money", "ban_trade_to_ticks",
        "familyid", "familyname", "familyrank", "roleprestige"
    };

            string query = $"SELECT {string.Join(",", columns)} FROM t_roles WHERE rid = @rid";
            if (bUseIsdel)
            {
                query += " AND isdel = 0";
            }
            query += " ORDER BY level DESC LIMIT 4";

            using (cmd = new MySqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@rid", roleID);

                using (var reader = cmd.ExecuteReader())
                {
                    if (!reader.HasRows)
                        return false;

                    if (reader.Read())
                    {
                        DBRoleInfo.DBTableRow2RoleInfo(this, reader); // dùng hàm đã viết trước
                    }
                }
            }


            if (GameDBManager.Flag_Splite_RoleParams_Table == 0)
            {
                DBRoleInfo.DBTableRow2RoleInfo_Params(this, conn, "t_roleparams", roleID, true);
            }
            else
            {
                DBRoleInfo.DBTableRow2RoleInfo_Params(this, conn, "t_roleparams_2", roleID, false);
                DBRoleInfo.DBTableRow2RoleInfo_ParamsEx(this, roleID);
            }

            DBRoleInfo.DBTableRow2RoleInfo_OldTasks(this, conn, roleID);

            DBRoleInfo.DBTableRow2RoleInfo_DoingTasks(this, conn, roleID);

            //Cầu hình đọc ra vật phaarmm trong DB
            DBRoleInfo.DBTableRow2RoleInfo_Goods(this, conn, roleID);

            DBRoleInfo.DBTableRow2RoleInfo_Friends(this, roleID);

            DBRoleInfo.DBTableRow2RoleInfo_Skills(this, conn, roleID);

            DBRoleInfo.DBTableRow2RoleInfo_Buffers(this, conn, roleID);

            DBRoleInfo.DBTableRow2RoleInfo_DailyTasks(this, conn, roleID);

            DBRoleInfo.DBTableRow2RoleInfo_PortableBag(this, conn, roleID);

            DBRoleInfo.DBTableRow2RoleInfo_HuodongData(this, conn, roleID);

            DBRoleInfo.DBTableRow2RoleInfo_DailyData(this, conn, roleID);

            DBRoleInfo.DBTableRow2RoleInfo_GMailInfo(this, conn, roleID);

            DBRoleInfo.DBTableRow2RoleInfo_SevenDayActData(this, conn, roleID);

            DBRoleInfo.DBTableRow2RoleInfo_SpecialActivityData(this, conn, roleID);

            cmd = null;

            RankValue.Init(roleID);
            return true;
        }

        #endregion 从数据库查询信息
    }
}
