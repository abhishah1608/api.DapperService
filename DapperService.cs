using Dapper;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;
using Z.Dapper.Plus;

namespace api.DapperService
{
    /// <summary>
    /// DapperService. 
    /// </summary>
    public class DapperService : IDapperService, IDisposable
    {

        private readonly SqlConnection _IDbConnection = null;

        /// <summary>
        ///  
        /// </summary>
        /// <param name="sqlConnection"></param>
        public DapperService(SqlConnection sqlConnection)
        {
            _IDbConnection = sqlConnection;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public int ExecuteScalar(string sql, DynamicParameters parameters)
        {
            int result = 0;
            _IDbConnection.Open();
            // Use the Query method to execute the query and return a list of objects    
            result = _IDbConnection.ExecuteScalar<int>(sql, parameters);
            _IDbConnection.Close();
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
            _IDbConnection.Open();
            // Use the Query method to execute the query and return a list of objects    
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
            result = _IDbConnection.ExecuteScalar<T>(sql, parameters);
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
            _IDbConnection.Close();
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<int> ExecuteScalarAsync(string sql, DynamicParameters parameters)
        {
            await _IDbConnection.OpenAsync(); // Asynchronously open the database connection

            // Execute the scalar query asynchronously
            int result = await _IDbConnection.ExecuteScalarAsync<int>(sql, parameters);
            await _IDbConnection.CloseAsync();
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<T> ExecuteScalarAsync<T>(string sql, DynamicParameters parameters) where T : class
        {
            await _IDbConnection.OpenAsync(); // Asynchronously open the database connection

            // Execute the scalar query asynchronously
            T result= await _IDbConnection.ExecuteScalarAsync<T>(sql, parameters);
            await _IDbConnection.CloseAsync();
            return result;
        }

        public async Task<T> ExecuteScalarAsync<T>(string sql, T obj) where T : class
        {
            await _IDbConnection.OpenAsync(); // Asynchronously open the database connection

            // Execute the scalar query asynchronously
            T result = await _IDbConnection.ExecuteScalarAsync<T>(sql, obj);
            await _IDbConnection.CloseAsync();
            return result;
        }


        public List<T> Query<T>(string sql, DynamicParameters parameters) where T : class
        {
            _IDbConnection.Open();
            List<T> result = _IDbConnection.Query<T>(sql, parameters)?.ToList();
            _IDbConnection.Close();
            return result;
        }

        public async Task<IEnumerable<T>> QueryAsync<T>(string sql, DynamicParameters parameters) where T : class
        {
            await _IDbConnection.OpenAsync();
            
            IEnumerable<T> result = await _IDbConnection.QueryAsync<T>(sql, parameters);

            await _IDbConnection.CloseAsync();

            return result;
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
            _IDbConnection.Open();
            T result =_IDbConnection.QueryFirst<T>(sql, parameters);
            _IDbConnection.Close();
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<T> QueryFirstAsync<T>(string sql, DynamicParameters parameters) where T : class
        {
            await _IDbConnection.OpenAsync();

            T result = await _IDbConnection.QueryFirstAsync<T>(sql, parameters);

            await _IDbConnection.CloseAsync();

            return result;
        }



        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public T QuerySingle<T>(string sql, DynamicParameters parameters) where T : class
        {
            _IDbConnection.Open();
            T result = _IDbConnection.QuerySingle<T>(sql, parameters);
            return result;
        }

        /// <summary>
        ///  
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<T> QuerySingleAsync<T>(string sql, DynamicParameters parameters) where T : class
        {
            await _IDbConnection.OpenAsync();
            
            T result = await _IDbConnection.QuerySingleAsync<T>(sql, parameters);
            
            await _IDbConnection.CloseAsync();
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public T QuerySingleOrDefault<T>(string sql, DynamicParameters parameters) where T : class
        {
            _IDbConnection.Open();
            T result = _IDbConnection.QuerySingleOrDefault<T>(sql, parameters);
            _IDbConnection.Close();
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<T> QuerySingleOrDefaultAsync<T>(string sql, DynamicParameters parameters) where T : class
        {
            await _IDbConnection.OpenAsync();
            T result = await _IDbConnection?.QuerySingleOrDefaultAsync<T>(sql, parameters);
            await _IDbConnection.CloseAsync();
            return result;
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
            _IDbConnection.Open();
            using (var transaction = _IDbConnection.BeginTransaction())
            {
                try
                {
                    result = _IDbConnection.Execute(sql, parameters, transaction);
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw ex;
                }
                finally
                {
                }
            }
            _IDbConnection.Close();
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
            await _IDbConnection.OpenAsync();
            using (var transaction = _IDbConnection.BeginTransaction())
            {
                try
                {
                    result = await _IDbConnection.ExecuteAsync(sql, parameters, transaction);
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw ex;
                }
                finally
                {

                }
            }
            return result;
        }


        public async Task<int> ExecuteAsync<T>(string sql, T obj)
        {
            int result = 0;
            await _IDbConnection.OpenAsync();
            using (var transaction = _IDbConnection.BeginTransaction())
            {
                try
                {
                    result = await _IDbConnection.ExecuteAsync(sql, obj, transaction);
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw ex;
                }
                finally
                {

                }

            }
            _IDbConnection.CloseAsync();
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="T4"></typeparam>
        /// <typeparam name="T5"></typeparam>
        /// <typeparam name="T6"></typeparam>
        /// <typeparam name="T7"></typeparam>
        /// <typeparam name="T8"></typeparam>
        /// <typeparam name="T9"></typeparam>
        /// <typeparam name="T10"></typeparam>
        /// <typeparam name="T11"></typeparam>
        /// <typeparam name="T12"></typeparam>
        /// <typeparam name="T13"></typeparam>
        /// <typeparam name="T14"></typeparam>
        /// <typeparam name="T15"></typeparam>
        /// <typeparam name="T16"></typeparam>
        /// <typeparam name="T17"></typeparam>
        /// <typeparam name="T18"></typeparam>
        /// <typeparam name="T19"></typeparam>
        /// <typeparam name="T20"></typeparam>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <param name="types"></param>
        /// <returns></returns>
        public async Task<T> QueryMultiple<T, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>(string sql, DynamicParameters parameters, params Type[] types) where T : new()
        {
            T t = new T();
            Type ty = typeof(T);
            PropertyInfo[] propertyList = ty.GetProperties();

            await _IDbConnection.OpenAsync();
            var results = await _IDbConnection.QueryMultipleAsync(sql, parameters);

            for (int i = 0; i < types.Length; i++)
            {
                Type type = types[i];
                var t1 = (dynamic)null;
                PropertyInfo prop = GetPropertyInfo(propertyList, type);
                bool singRecord = false;
                if (prop == null)
                {
                    singRecord = true;
                    prop = GetNonListProperty(propertyList, type);
                }

                if (typeof(T1) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T1>() : await results.ReadAsync<T1>();
                }
                else if (typeof(T2) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T2>() : await results.ReadAsync<T2>();
                }
                else if (typeof(T3) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T3>() : await results.ReadAsync<T3>();
                }
                else if (typeof(T4) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T4>() : await results.ReadAsync<T4>();
                }
                else if (typeof(T5) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T5>() : await results.ReadAsync<T5>();
                }
                else if (typeof(T6) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T6>() : await results.ReadAsync<T6>();
                }
                else if (typeof(T7) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T7>() : await results.ReadAsync<T7>();
                }
                else if (typeof(T8) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T8>() : await results.ReadAsync<T8>();
                }
                else if (typeof(T9) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T9>() : await results.ReadAsync<T9>();
                }
                else if (typeof(T10) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T10>() : await results.ReadAsync<T10>();
                }
                else if (typeof(T11) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T11>() : await results.ReadAsync<T11>();
                }
                else if (typeof(T12) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T12>() : await results.ReadAsync<T12>();
                }
                else if (typeof(T13) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T13>() : await results.ReadAsync<T13>();
                }
                else if (typeof(T14) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T14>() : await results.ReadAsync<T14>();
                }
                else if (typeof(T15) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T15>() : await results.ReadAsync<T15>();
                }
                else if (typeof(T16) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T16>() : await results.ReadAsync<T16>();
                }
                else if (typeof(T17) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T17>() : await results.ReadAsync<T17>();
                }
                else if (typeof(T18) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T18>() : await results.ReadAsync<T18>();
                }
                else if (typeof(T19) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T19>() : await results.ReadAsync<T19>();
                }
                else if (typeof(T20) == type)
                {
                    t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T20>() : await results.ReadAsync<T20>();
                }

                if (t1 != null)
                {
                    if (prop != null && prop.CanWrite)
                    {
                        prop.SetValue(t, t1);
                    }
                }

            }

            _IDbConnection.CloseAsync();
            return await Task.FromResult(t);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="T4"></typeparam>
        /// <typeparam name="T5"></typeparam>
        /// <typeparam name="T6"></typeparam>
        /// <typeparam name="T7"></typeparam>
        /// <typeparam name="T8"></typeparam>
        /// <typeparam name="T9"></typeparam>
        /// <typeparam name="T10"></typeparam>
        /// <typeparam name="T11"></typeparam>
        /// <typeparam name="T12"></typeparam>
        /// <typeparam name="T13"></typeparam>
        /// <typeparam name="T14"></typeparam>
        /// <typeparam name="T15"></typeparam>
        /// <typeparam name="T16"></typeparam>
        /// <typeparam name="T17"></typeparam>
        /// <typeparam name="T18"></typeparam>
        /// <typeparam name="T19"></typeparam>
        /// <typeparam name="T20"></typeparam>
        /// <param name="spname"></param>
        /// <param name="parameters"></param>
        /// <param name="types"></param>
        /// <returns></returns>
        public async Task<T> ExecuteStoredProcedure<T, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>(string spname, DynamicParameters parameters, params Type[] types) where T : new()
        {
            T t = new T();
            Type ty = typeof(T);
            PropertyInfo[] propertyList = ty.GetProperties();

            //Set up DynamicParameters object to pass parameters  
            await _IDbConnection.OpenAsync();

            using (var results = _IDbConnection.QueryMultiple(spname, parameters, commandType: CommandType.StoredProcedure))
            {
                // Anonymous Type
                for (int i = 0; i < types.Length; i++)
                {
                    Type type = types[i];
                    bool singRecord = false;
                    PropertyInfo prop = GetPropertyInfo(propertyList, type);
                    if (prop == null)
                    {
                        singRecord = true;
                        prop = GetNonListProperty(propertyList, type);
                    }
                    var t1 = (dynamic)null;
                    if (typeof(T1) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T1>() : await results.ReadAsync<T1>();
                    }
                    else if (typeof(T2) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T2>() : await results.ReadAsync<T2>();
                    }
                    else if (typeof(T3) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T3>() : await results.ReadAsync<T3>();
                    }
                    else if (typeof(T4) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T4>() : await results.ReadAsync<T4>();
                    }
                    else if (typeof(T5) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T5>() : await results.ReadAsync<T5>();
                    }
                    else if (typeof(T6) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T6>() : await results.ReadAsync<T6>();
                    }
                    else if (typeof(T7) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T7>() : await results.ReadAsync<T7>();
                    }
                    else if (typeof(T8) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T8>() : await results.ReadAsync<T8>();
                    }
                    else if (typeof(T9) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T9>() : await results.ReadAsync<T9>();
                    }
                    else if (typeof(T10) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T10>() : await results.ReadAsync<T10>();
                    }
                    else if (typeof(T11) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T11>() : await results.ReadAsync<T11>();
                    }
                    else if (typeof(T12) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T12>() : await results.ReadAsync<T12>();
                    }
                    else if (typeof(T13) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T13>() : await results.ReadAsync<T13>();
                    }
                    else if (typeof(T14) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T14>() : await results.ReadAsync<T14>();
                    }
                    else if (typeof(T15) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T15>() : await results.ReadAsync<T15>();
                    }
                    else if (typeof(T16) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T16>() : await results.ReadAsync<T16>();
                    }
                    else if (typeof(T17) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T17>() : await results.ReadAsync<T17>();
                    }
                    else if (typeof(T18) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T18>() : await results.ReadAsync<T18>();
                    }
                    else if (typeof(T19) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T19>() : await results.ReadAsync<T19>();
                    }
                    else if (typeof(T20) == type)
                    {
                        t1 = singRecord ? await results.ReadFirstOrDefaultAsync<T20>() : await results.ReadAsync<T20>();
                    }

                    if (t1 != null)
                    {
                        if (prop != null && prop.CanWrite)
                        {
                            prop.SetValue(t, t1);
                        }
                    }
                }
            }

            _IDbConnection.CloseAsync();

            return await Task.FromResult(t);
        }

        public int BulkInsert<T>(string query, List<T> records) where T : class
        {
            int result = 0;
            _IDbConnection.Open();
            _IDbConnection.BulkInsert<T>(records);
            _IDbConnection.Close();
            return result;
        }

        public async Task<int> BulkInsertAsync<T>(string query, List<T> records) where T : class
        {
            int result = 0;
            await _IDbConnection.OpenAsync();
            await _IDbConnection.BulkInsertAsync<T>(records);
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
            PropertyInfo info = null;
            if (propertyList != null)
            {
                info = propertyList.Where(p => p.PropertyType.IsGenericType && p.PropertyType.GetGenericTypeDefinition().IsAssignableFrom(typeof(List<>)) && p.PropertyType.GetGenericArguments()[0] == type).FirstOrDefault();
            }
            return info;
        }

        private PropertyInfo GetNonListProperty(PropertyInfo[] propertyList, Type type) {
            PropertyInfo info = null;
            if (propertyList != null)
            {
                info = propertyList.Where(p => !p.PropertyType.IsGenericType && p.PropertyType == type).FirstOrDefault();
            }
            return info;
        }

        public void Dispose()
        {
            // dispose free resources.
        }
    }
}