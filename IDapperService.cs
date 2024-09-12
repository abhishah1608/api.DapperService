using Dapper;

namespace api.DapperService
{
    /// <summary>
    /// Author : Abhi Shah : Create various Dapper Operations.
    /// </summary>
    public interface IDapperService
    {

        #region scalar Queries

        /// <summary>
        /// string sql.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        int ExecuteScalar(string sql, DynamicParameters paramters);

        /// <summary>
        /// string sql.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        Task<int> ExecuteScalarAsync(string sql, DynamicParameters parameters);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="obj"></param>
        /// <returns></returns>
        Task<T> ExecuteScalarAsync<T>(string sql, T obj) where T : class;

        /// <summary>
        /// string sql.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        T ExecuteScalar<T>(string sql, DynamicParameters parameters) where T : class;

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        Task<T> ExecuteScalarAsync<T>(string sql, DynamicParameters parameters) where T : class;

        #endregion scalar Queries

        #region Single Row

        T QuerySingle<T>(string sql, DynamicParameters parameters) where T : class;

        T QuerySingleOrDefault<T>(string sql, DynamicParameters parameters) where T : class;

        T QueryFirst<T>(string sql, DynamicParameters parameters) where T : class;

        Task<T> QuerySingleAsync<T>(string sql, DynamicParameters parameters) where T : class;

        Task<T> QuerySingleOrDefaultAsync<T>(string sql, DynamicParameters parameters) where T : class;

        Task<T> QueryFirstAsync<T>(string sql, DynamicParameters parameters) where T : class;


        #endregion Single Row

        #region multiple rows
        List<T> Query<T>(string sql, DynamicParameters parameters) where T : class;

        Task<IEnumerable<T>> QueryAsync<T>(string sql, DynamicParameters parameters) where T : class;

        #endregion multiple rows

        #region NonQuery Commnads - Insert, Update and delete

        public int Execute(string sql, DynamicParameters parameters);
        public Task<int> ExecuteAsync(string sql, DynamicParameters parameters);

        public Task<int> ExecuteAsync<T>(string sql, T obj);

        #endregion NonQuery Commnads - Insert, Update and delete

        public Task<T> QueryMultiple<T, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>(string sql, DynamicParameters parameters, params Type[] types) where T : new();

        public Task<T> ExecuteStoredProcedure<T, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>(string spname, DynamicParameters parameters, params Type[] types) where T : new();

        #region Execute non-Query commands.
        public int BulkInsert<T>(string query, List<T> records) where T : class;

        public Task<int> BulkInsertAsync<T>(string query, List<T> records) where T : class;

        #endregion Execute non-Query commands.

        public void CustomBulkUpdate<T>(List<T> entities, List<string> propertieNames) where T : class;

        public void CustomBulkUpdateAsync<T>(List<T> entities, List<string> propertieNames) where T : class;

    }
}
