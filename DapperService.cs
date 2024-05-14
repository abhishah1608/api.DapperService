using Dapper;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;
using Z.Dapper.Plus;

namespace api.DapperService
{
    /// <summary>
    /// DapperService. 
    /// </summary>
    public class DapperService : IDapperService
    {

        private string connectionstring = null;

        /// <summary>
        ///  
        /// </summary>
        /// <param name="sqlConnection"></param>
        public DapperService(string sqlConnection)
        {
            connectionstring = sqlConnection;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public int ExecuteScalar(string sql, DynamicParameters parameters)
        {
            int result = 0;
            using (var connection = new SqlConnection(connectionstring))
            {
                connection.Open();
                // Use the Query method to execute the query and return a list of objects    
                result = connection.ExecuteScalar<int>(sql, parameters);
            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string sql, DynamicParameters parameters) where T : class
        {
            T result;
            using (SqlConnection connection = new SqlConnection(connectionstring))
            {
                // Use the Query method to execute the query and return a list of objects    
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
                result = connection.ExecuteScalar<T>(sql, parameters);
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<int> ExecuteScalarAsync(string sql, DynamicParameters parameters)
        {
            using (var connection = new SqlConnection(connectionstring))
            {
                await connection.OpenAsync(); // Asynchronously open the database connection

                // Execute the scalar query asynchronously
                return await connection.ExecuteScalarAsync<int>(sql, parameters);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<T> ExecuteScalarAsync<T>(string sql, DynamicParameters parameters) where T : class
        {
            using (var connection = new SqlConnection(connectionstring))
            {
                await connection.OpenAsync(); // Asynchronously open the database connection

                // Execute the scalar query asynchronously
                return await connection.ExecuteScalarAsync<T>(sql, parameters);
            }
        }



        public List<T> Query<T>(string sql, DynamicParameters parameters) where T : class
        {
            using (var connection = new SqlConnection(connectionstring))
            {
                return connection.Query<T>(sql, parameters)?.ToList();
            }
        }

        public async Task<IEnumerable<T>> QueryAsync<T>(string sql, DynamicParameters parameters) where T : class
        {
            using (var connection = new SqlConnection(connectionstring))
            {
                await connection.OpenAsync();
                return await connection.QueryAsync<T>(sql, parameters);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public T QueryFirst<T>(string sql, DynamicParameters parameters) where T : class
        {
            using (var connection = new SqlConnection(connectionstring))
            {
                return connection.QueryFirst<T>(sql, parameters);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<T> QueryFirstAsync<T>(string sql, DynamicParameters parameters) where T : class
        {
            using (var connection = new SqlConnection(connectionstring))
            {
                await connection.OpenAsync();
                return await connection.QueryFirstAsync<T>(sql, parameters);
            }
        }



        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public T QuerySingle<T>(string sql, DynamicParameters parameters) where T : class
        {
            using (var connection = new SqlConnection(connectionstring))
            {
                return connection.QuerySingle<T>(sql, parameters);
            }
        }

        /// <summary>
        ///  
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<T> QuerySingleAsync<T>(string sql, DynamicParameters parameters) where T : class
        {
            using (var connection = new SqlConnection(connectionstring))
            {
                await connection.OpenAsync();
                return await connection.QuerySingleAsync<T>(sql, parameters);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public T QuerySingleOrDefault<T>(string sql, DynamicParameters parameters) where T : class
        {
            using (var connection = new SqlConnection(connectionstring))
            {
                return connection.QuerySingleOrDefault<T>(sql, parameters);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<T> QuerySingleOrDefaultAsync<T>(string sql, DynamicParameters parameters) where T : class
        {
            using (var connection = new SqlConnection(connectionstring))
            {
                await connection.OpenAsync();
                return await connection?.QuerySingleOrDefaultAsync<T>(sql, parameters);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public int Execute(string sql, DynamicParameters parameters)
        {
            int result = 0;
            using (var connection = new SqlConnection(connectionstring))
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        result = connection.Execute(sql, parameters, transaction);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                    }
                    finally
                    {
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public async Task<int> ExecuteAsync(string sql, DynamicParameters parameters)
        {
            int result = 0;
            using (var connection = new SqlConnection(connectionstring))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        result = await connection.ExecuteAsync(sql, parameters, transaction);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                    }
                    finally
                    {

                    }

                }
                return result;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <param name="listInfo"></param>
        /// <param name="types"></param>
        /// <returns></returns>
        public async Task<T> QueryMultiple<T, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>(string sql, DynamicParameters parameters, params Type[] types) where T : new()
        {
            T t = new T();
            Type ty = typeof(T);
            PropertyInfo[] propertyList = ty.GetProperties();

            using (var connection = new SqlConnection(connectionstring))
            {
                await connection.OpenAsync();
                var results = await connection.QueryMultipleAsync(sql, parameters);

                for (int i = 0; i < types.Length; i++)
                {
                    Type type = types[i];
                    var t1 = (dynamic)null;
                    if (typeof(T1) == type)
                    {
                        t1 = await results.ReadAsync<T1>();
                    }
                    else if (typeof(T2) == type)
                    {
                        t1 = await results.ReadAsync<T2>();
                    }
                    else if (typeof(T3) == type)
                    {
                        t1 = await results.ReadAsync<T3>();
                    }
                    else if (typeof(T4) == type)
                    {
                        t1 = await results.ReadAsync<T4>();
                    }
                    else if (typeof(T5) == type)
                    {
                        t1 = await results.ReadAsync<T5>();
                    }
                    else if (typeof(T6) == type)
                    {
                        t1 = await results.ReadAsync<T6>();
                    }
                    else if (typeof(T7) == type)
                    {
                        t1 = await results.ReadAsync<T7>();
                    }
                    else if (typeof(T8) == type)
                    {
                        t1 = await results.ReadAsync<T8>();
                    }
                    else if (typeof(T9) == type)
                    {
                        t1 = await results.ReadAsync<T9>();
                    }
                    else if (typeof(T10) == type)
                    {
                        t1 = await results.ReadAsync<T10>();
                    }
                    else if (typeof(T11) == type)
                    {
                        t1 = await results.ReadAsync<T11>();
                    }
                    else if (typeof(T12) == type)
                    {
                        t1 = await results.ReadAsync<T12>();
                    }
                    else if (typeof(T13) == type)
                    {
                        t1 = await results.ReadAsync<T13>();
                    }
                    else if (typeof(T14) == type)
                    {
                        t1 = await results.ReadAsync<T14>();
                    }
                    else if (typeof(T15) == type)
                    {
                        t1 = await results.ReadAsync<T15>();
                    }
                    else if (typeof(T16) == type)
                    {
                        t1 = await results.ReadAsync<T16>();
                    }
                    else if (typeof(T17) == type)
                    {
                        t1 = await results.ReadAsync<T17>();
                    }
                    else if (typeof(T18) == type)
                    {
                        t1 = await results.ReadAsync<T18>();
                    }
                    else if (typeof(T19) == type)
                    {
                        t1 = await results.ReadAsync<T19>();
                    }
                    else if (typeof(T20) == type)
                    {
                        t1 = await results.ReadAsync<T20>();
                    }

                    if (t1 != null)
                    {
                        PropertyInfo prop = GetPropertyInfo(propertyList, type);
                        if (prop != null && prop.CanWrite)
                        {
                            prop.SetValue(t, t1);
                        }
                    }
                }
            }
            return await Task.FromResult(t);
        }

        public async Task<T> ExecuteStoredProcedure<T, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>(string spname, DynamicParameters parameters, params Type[] types) where T : new()
        {
            T t = new T();
            Type ty = typeof(T);
            PropertyInfo[] propertyList = ty.GetProperties();

            using (var connection = new SqlConnection(connectionstring))
            {
                //Set up DynamicParameters object to pass parameters  
                await connection.OpenAsync();


                using (var results = connection.QueryMultiple(spname, parameters, commandType: CommandType.StoredProcedure))
                {
                    // Anonymous Type
                    for (int i = 0; i < types.Length; i++)
                    {
                        Type type = types[i];
                        var t1 = (dynamic)null;
                        if (typeof(T1) == type)
                        {
                            t1 = await results.ReadAsync<T1>();
                        }
                        else if (typeof(T2) == type)
                        {
                            t1 = await results.ReadAsync<T2>();
                        }
                        else if (typeof(T3) == type)
                        {
                            t1 = await results.ReadAsync<T3>();
                        }
                        else if (typeof(T4) == type)
                        {
                            t1 = await results.ReadAsync<T4>();
                        }
                        else if (typeof(T5) == type)
                        {
                            t1 = await results.ReadAsync<T5>();
                        }
                        else if (typeof(T6) == type)
                        {
                            t1 = await results.ReadAsync<T6>();
                        }
                        else if (typeof(T7) == type)
                        {
                            t1 = await results.ReadAsync<T7>();
                        }
                        else if (typeof(T8) == type)
                        {
                            t1 = await results.ReadAsync<T8>();
                        }
                        else if (typeof(T9) == type)
                        {
                            t1 = await results.ReadAsync<T9>();
                        }
                        else if (typeof(T10) == type)
                        {
                            t1 = await results.ReadAsync<T10>();
                        }
                        else if (typeof(T11) == type)
                        {
                            t1 = await results.ReadAsync<T11>();
                        }
                        else if (typeof(T12) == type)
                        {
                            t1 = await results.ReadAsync<T12>();
                        }
                        else if (typeof(T13) == type)
                        {
                            t1 = await results.ReadAsync<T13>();
                        }
                        else if (typeof(T14) == type)
                        {
                            t1 = await results.ReadAsync<T14>();
                        }
                        else if (typeof(T15) == type)
                        {
                            t1 = await results.ReadAsync<T15>();
                        }
                        else if (typeof(T16) == type)
                        {
                            t1 = await results.ReadAsync<T16>();
                        }
                        else if (typeof(T17) == type)
                        {
                            t1 = await results.ReadAsync<T17>();
                        }
                        else if (typeof(T18) == type)
                        {
                            t1 = await results.ReadAsync<T18>();
                        }
                        else if (typeof(T19) == type)
                        {
                            t1 = await results.ReadAsync<T19>();
                        }
                        else if (typeof(T20) == type)
                        {
                            t1 = await results.ReadAsync<T20>();
                        }

                        if (t1 != null)
                        {
                            PropertyInfo prop = GetPropertyInfo(propertyList, type);
                            if (prop != null && prop.CanWrite)
                            {
                                prop.SetValue(t, t1);
                            }
                        }
                    }
                }

            }

            return await Task.FromResult(t);
        }

        public int BulkInsert<T>(string query, List<T> records) where T : class
        {
            int result = 0;
            using (var connection = new SqlConnection(connectionstring))
            {
                connection.Open();
                connection.BulkInsert<T>(records);
            }
            return result;
        }

        public async Task<int> BulkInsertAsync<T>(string query, List<T> records) where T : class
        {
            int result = 0;
            using (var connection = new SqlConnection(connectionstring))
            {
                await connection.OpenAsync();
                await connection.BulkInsertAsync<T>(records);
            }
            return result;
        }

        public void CustomBulkUpdate<T>(List<T> entities, List<string> propertieNames) where T : class
        {
            var context = new DapperPlusContext();

            propertieNames.ForEach(x =>
            {
                context.Entity<T>().Map(x);
            });

            context.BulkUpdate<T>(entities);
        }

        public void CustomBulkUpdateAsync<T>(List<T> entities, List<string> propertieNames) where T : class
        {
            var context = new DapperPlusContext();

            propertieNames.ForEach(x =>
            {
                context.Entity<T>().Map(x);
            });

            context.BulkUpdateAsync<T>(entities);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="propertyList"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        private PropertyInfo GetPropertyInfo(PropertyInfo[] propertyList, Type type)
        {
            PropertyInfo info = default;
            if (propertyList != null)
            {
                info = propertyList.Where(p => p.PropertyType.IsGenericType && p.PropertyType.GetGenericTypeDefinition().IsAssignableFrom(typeof(List<>)) && p.PropertyType.GetGenericArguments()[0] == type).FirstOrDefault();
            }
            return info;
        }
    }
}