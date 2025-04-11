using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using GameDBServer.DB;
using GameDBServer.Logic;
using MySql.Data.MySqlClient;
using Server.Tools;

namespace GameDBServer.DB.DBController
{

    public class DatabaseHelper
    {
        private readonly DBManager dbMgr;

        public DatabaseHelper(DBManager dbManager)
        {
            dbMgr = dbManager;
        }

        // Truy vấn một đối tượng từ cơ sở dữ liệu
        public T? QueryForObject<T>(string sql)
        {
            MySqlConnection? conn = null;
            T? obj = default(T);  // Khởi tạo đối tượng mặc định kiểu T
            try
            {
                // Lấy kết nối từ pool
                conn = dbMgr.DBConns.PopDBConnection();

                // Sử dụng parameterized query nếu có tham số
                MySqlCommand cmd = new MySqlCommand(sql, conn);

                using (MySqlDataReader reader = cmd.ExecuteReader())
                {
                    int columnNum = reader.FieldCount;

                    if (reader.Read())  // Nếu có ít nhất 1 dòng dữ liệu
                    {
                        // Khởi tạo đối tượng T nếu chưa có
                        if (obj == null)
                            obj = Activator.CreateInstance<T>();

                        // Duyệt qua các cột trong dòng dữ liệu
                        for (int i = 0; i < columnNum; i++)
                        {
                            string columnName = reader.GetName(i);
                            object columnValue = reader.IsDBNull(i) ? null : reader.GetValue(i);  // Kiểm tra NULL

                            // Gán giá trị vào đối tượng
                            SetValue(obj, columnName, columnValue);
                        }
                    }
                }

                // Log câu truy vấn SQL
                GameDBManager.SystemServerSQLEvents.AddEvent(string.Format("+SQL: {0}", sql), EventLevels.Important);
            }
            catch (Exception ex)
            {
                // Log lỗi nếu có exception
                LogManager.WriteLog(LogTypes.Error, string.Format("查询数据库失败: {0} - 错误: {1}", sql, ex.Message));
                return default(T);  // Trả về giá trị mặc định nếu có lỗi
            }
            finally
            {
                // Đảm bảo luôn trả kết nối về pool
                if (conn != null)
                {
                    dbMgr.DBConns.PushDBConnection(conn);
                }
            }

            return obj;
        }

        // Hàm gán giá trị vào thuộc tính của đối tượng
        private void SetValue<T>(T obj, string columnName, object columnValue)
        {
            // Sử dụng Reflection để tìm thuộc tính của đối tượng và gán giá trị
            var property = typeof(T).GetProperty(columnName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

            if (property != null && columnValue != DBNull.Value)
            {
                // Kiểm tra kiểu dữ liệu và gán giá trị
                property.SetValue(obj, Convert.ChangeType(columnValue, property.PropertyType));
            }
        }
    }

}

