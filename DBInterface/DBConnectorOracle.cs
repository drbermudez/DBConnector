using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Runtime.CompilerServices;
using System.Diagnostics;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace DBInterface
{
    /// <summary>
    /// Public class that enables connection to an Oracle SQL data base and execute commands.
    /// </summary>
    public class DBConnectorOracle: IDisposable
    {
        private OracleConnectionStringBuilder connectionString;
        private List<OracleParameter> Parameters { get; set; }
        private bool disposed = false; //used for the Dispose method

        /// <summary>
        /// Gets a list of Error objects with error messages
        /// </summary>
        public List<Error> ErrorList { get; private set; }
        
        /// <summary>
        /// Main constructor
        /// </summary>
        /// <param name="dataSource">Server name</param>
        /// <param name="initialCatalog">Data base name</param>
        /// <param name="userId">User Id</param>
        /// <param name="passWord">Password</param>
        /// <param name="persistSecurityInfo">Whether to keep password in memory or not</param>
        public DBConnectorOracle(string dataSource, string initialCatalog, string userId, string passWord, bool persistSecurityInfo, bool integratedSecurity)
        {
            connectionString = new OracleConnectionStringBuilder();
            connectionString.DataSource = dataSource;            
            if (integratedSecurity)
            {
                connectionString.UserID = "/";
                connectionString.Password = "";
            }
            else
            {
                connectionString.UserID = userId;
                connectionString.Password = passWord;
            }
            connectionString.PersistSecurityInfo = persistSecurityInfo;
            connectionString.ConnectionTimeout = 40;
            Parameters = new List<OracleParameter>();
            ErrorList = new List<Error>();
        }

        /// <summary>
        /// Main constructor
        /// </summary>
        /// <param name="connString">A connection string to the database</param>
        public DBConnectorOracle(string connString)
        {
            connectionString = new OracleConnectionStringBuilder(connString);
            Parameters = new List<OracleParameter>();
            ErrorList = new List<Error>();
        }

        /// <summary>
        /// Clears the list of SQL Parameters and the list of Errors
        /// </summary>
        public void Clear()
        {
            Parameters.Clear();
            ErrorList.Clear();
        }

        /// <summary>
        /// Add a parameter to the Sql Command
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="type">Type of parameter in .Net</param>
        /// <param name="dbType">Type of parameter in SQL Server</param>
        /// <param name="direction">Parameter direction (default is input)</param>
        /// <param name="value">Value for the parameter</param>
        public void AddParameter(string name, object value, DbType type, OracleDbType dbType, ParameterDirection direction = ParameterDirection.Input)
        {
            OracleParameter parameter = new OracleParameter();
            parameter.DbType = type;
            parameter.Direction = direction;
            parameter.ParameterName = name;
            parameter.OracleDbType = dbType;
            parameter.Value = value;
            if (!Parameters.Contains(parameter))
            {
                Parameters.Add(parameter);
            }
        }

        /// <summary>
        /// Add a list of parameters to the Sql Command
        /// </summary>
        /// <param name="parameters">List of SQL Parameters</param>
        public void AddParameters(List<OracleParameter> parameters)
        {
            foreach(OracleParameter parameter in parameters)
            {
                if (!Parameters.Contains(parameter))
                {
                    Parameters.Add(parameter);
                }
            }
        }

        /// <summary>
        /// Get whether the connection can be established or not
        /// </summary>
        public bool CanConnect()
        {
            bool canConnect = false;
            try
            {
                using (OracleConnection connection = new OracleConnection(connectionString.ConnectionString))
                {
                    connection.Open();
                    canConnect = Convert.ToBoolean(connection.State == ConnectionState.Open);
                    connection.Close();
                }                                
            }
            catch (Exception ex)
            {
                Error aError = new Error(ex.Source, ex.Message, GetCurrentMethod());
                ErrorList.Add(aError);
                canConnect = false;
            }
            return canConnect;
        }

        /// <summary>
        /// Execute SQL command and return the number of rows affected
        /// </summary>
        /// <param name="command">Command (Text command or Stored Procedure)</param>
        /// <param name="type">Type of command (text, stored procedure or table-direct)</param>
        /// <returns>Number of rows affected</returns>
        public int ExecuteNonQuery(string command, CommandType type)
        {
            int rowsAffected = 0;
            try
            {
                using (OracleConnection connection = new OracleConnection(connectionString.ConnectionString))
                {
                    using (OracleCommand cmd = new OracleCommand(command))
                    {
                        cmd.Connection = connection;
                        foreach(OracleParameter parameter in Parameters)
                        {
                            cmd.Parameters.Add(parameter);
                        }
                        cmd.CommandType = type;
                        cmd.Connection.Open();
                        rowsAffected = cmd.ExecuteNonQuery();
                        cmd.Connection.Close();
                    }                    
                }
            }
            catch (Exception ex)
            {
                Error aError = new Error(ex.Source, ex.Message, GetCurrentMethod());
                ErrorList.Add(aError);
            }
            return rowsAffected;
        }
        
        /// <summary>
        /// Returns the first column of the first row in the executed query. Additional rows and columns are ignored.
        /// </summary>
        /// <param name="command">Command (Text command or Stored Procedure)</param>
        /// <param name="type">Type of command (text, stored procedure or table-direct)</param>
        /// <returns></returns>
        public object ExecuteScalar(string command, CommandType type)
        {
            object value = null;
            try
            {
                using (OracleConnection connection = new OracleConnection(connectionString.ConnectionString))
                {
                    using (OracleCommand cmd = new OracleCommand(command))
                    {
                        cmd.Connection = connection;
                        foreach (OracleParameter parameter in Parameters)
                        {
                            cmd.Parameters.Add(parameter);
                        }
                        cmd.CommandType = type;
                        cmd.Connection.Open();
                        value = cmd.ExecuteScalar();
                        cmd.Connection.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                Error aError = new Error(ex.Source, ex.Message, GetCurrentMethod());
                ErrorList.Add(aError);
            }
            return value;
        }

        /// <summary>
        /// Uses a SQL Data Reader to load a data table with data from the query execution
        /// </summary>
        /// <param name="command">Command (Text command or Stored Procedure)</param>
        /// <param name="type">Type of command (text, stored procedure or table-direct)</param>
        /// <returns>Data Table with the execution results</returns>
        public DataTable GetTable(string command, CommandType type)
        {            
            DataTable resultSet = new DataTable();
            try
            {
                using (OracleConnection connection = new OracleConnection(connectionString.ConnectionString))
                {
                    using (OracleCommand cmd = new OracleCommand(command))
                    {
                        cmd.Connection = connection;
                        foreach (OracleParameter parameter in Parameters)
                        {
                            cmd.Parameters.Add(parameter);
                        }
                        cmd.CommandType = type;
                        cmd.Connection.Open();

                        OracleDataReader reader = null;
                        reader = cmd.ExecuteReader(CommandBehavior.KeyInfo);

                        resultSet.Load(reader);
                        reader.Close();
                        cmd.Connection.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                Error aError = new Error(ex.Source, ex.Message, GetCurrentMethod());
                ErrorList.Add(aError);
            }
            return resultSet;
        }

        /// <summary>
        /// Uses a SQL Data Reader to load a data set with data tables from the query execution
        /// </summary>
        /// <param name="command">Command (Text command or Stored Procedure)</param>
        /// <param name="type">Type of command (text, stored procedure or table-direct)</param>
        /// <returns>Data Set with Data Tables containing the execution results</returns>
        public DataSet GetDataSet(string command, CommandType type)
        {
            DataSet resultSet = new DataSet();
            try
            {
                using (OracleConnection connection = new OracleConnection(connectionString.ConnectionString))
                {
                    using (OracleCommand cmd = new OracleCommand(command))
                    {
                        cmd.Connection = connection;
                        foreach (OracleParameter parameter in Parameters)
                        {
                            cmd.Parameters.Add(parameter);
                        }
                        cmd.CommandType = type;
                        cmd.Connection.Open();

                        OracleDataReader reader = null;
                        reader = cmd.ExecuteReader(CommandBehavior.KeyInfo);

                        do
                        {
                            DataTable table = new DataTable();
                            table.Load(reader);
                            resultSet.Tables.Add(table);
                        } while (!reader.IsClosed);

                        reader.Close();
                        cmd.Connection.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                Error aError = new Error(ex.Source, ex.Message, GetCurrentMethod());
                ErrorList.Add(aError);
            }
            return resultSet;
        }

        /// <summary>
        /// Disposes of any unmanaged resources or managed recources that implement IDisposable
        /// </summary>
        public void Dispose()
        {
            Dispose(true);

            // Use SupressFinalize in case a subclass 
            // of this type implements a finalizer.
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Overrides the native Didspose method
        /// </summary>
        /// <param name="disposing">True or False</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    // Clear all property values that maybe have been set
                    // when the class was instantiated 
                    Clear();
                    connectionString.Clear();
                    connectionString = null;
                }

                // Indicate that the instance has been disposed.
                disposed = true;
            }
        }

        /// <summary>
        /// Returns the name of the method or routine being executed
        /// </summary>
        /// <returns>Name of the method or routine as a string value</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private string GetCurrentMethod()
        {
            StackTrace st = new StackTrace();
            StackFrame sf = st.GetFrame(1);

            return sf.GetMethod().Name;
        }
    }
}