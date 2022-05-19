using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using System.Configuration;

namespace Dashboard.Utilities
{
	public class DBConnection
	{
        public static string _connectionString;

        public static string _connectionString_2;

        public static string GetConnectionString()
        {
            if (string.IsNullOrEmpty(_connectionString))
            {
                _connectionString = ConfigurationManager.ConnectionStrings["POSX_ConnectionString"].ConnectionString;
            }

            return _connectionString;
        }

        public static string GetConnectionString1()
        {
            if (string.IsNullOrEmpty(_connectionString))
            {
                _connectionString = ConfigurationManager.ConnectionStrings["MAP_ConnectionString"].ConnectionString;
            }

            return _connectionString;
        }

        public static string GetConnectionString2()
        {
            if (string.IsNullOrEmpty(_connectionString_2))
            {
                _connectionString_2 = ConfigurationManager.ConnectionStrings["POSXC_ConnectionString"].ConnectionString;
            }

            return _connectionString_2;
        }
    }
}


//