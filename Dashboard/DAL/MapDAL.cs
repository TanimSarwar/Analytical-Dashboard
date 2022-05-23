using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;
using Dashboard.Utilities;

namespace Dashboard.DAL
{
	public class MapDAL
	{
		public static SqlConnection conn = new SqlConnection(DBConnection.GetConnectionString());
		public static SqlConnection conn1 = new SqlConnection(DBConnection.GetConnectionString());

		public DataTable GetOutletDatas(string _SHORT_KEY, int _ORG_TYPE, int _FILT_TYPE)
		{
			DataTable dt = new DataTable();
			if (conn.State == 0)
			{
				conn.Open();
			}
			try
			{
				SqlCommand cmd = new SqlCommand("USP_GET_MAPDATA", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.Parameters.AddWithValue("@SHORT_KEY", _SHORT_KEY);
				cmd.Parameters.AddWithValue("@ORG_TYPE", _ORG_TYPE);
				cmd.Parameters.AddWithValue("@FilterType", _FILT_TYPE);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex;
			}

			//try
			//{
			//	if (conn.State == 0)
			//	{
			//		conn.Open();
			//	}


			//	string _sql = "SELECT EXTERNALID + ':' + ORG_NAME [Outlet_Name], * FROM OUTLET_LOC WHERE Lattitude <> 0 AND Division = '"+_div+"'";
			//	SqlCommand cmd = new SqlCommand(_sql, conn1);
			//	cmd.CommandType = System.Data.CommandType.Text;
			//	SqlDataAdapter da = new SqlDataAdapter(cmd);
			//	DataTable dt = new DataTable();
			//	da.Fill(dt);
			//	return dt;
			//}
			//catch (Exception ex)
			//{
			//	throw ex;
			//}
		}

		public DataTable sideInfo(string _SHORT_KEY, int _type)
		{


			try
			{
				if (conn.State == 0)
				{
					conn.Open();
				}
				string _sql = "";
				if (_type == 1)
				{
					_sql = "SELECT D.NAME,D.SHORT_KEY ,DI.[AREA],  DI.POPULATION,MALE_PER,FEMALE_PER FROM DIVISIONS_INFO DI LEFT JOIN DIVISIONS D ON D.DIVISIONSID = DI.DIVISION_ID WHERE D.SHORT_KEY = '" + _SHORT_KEY + "'";

				}
				if (_type == 2)
				{
					_sql = "SELECT * FROM DISTRICTS_INFO DI LEFT JOIN DISTRICTS D ON D.DISTRICTSID = DI.DISTRICTS_ID WHERE NAME = '" + _SHORT_KEY + "'";

				}
				SqlCommand cmd = new SqlCommand(_sql, conn1);
				cmd.CommandType = System.Data.CommandType.Text;
				SqlDataAdapter da = new SqlDataAdapter(cmd);
				DataTable dt = new DataTable();
				da.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex;
			}
		}

		public DataTable MAP_SALES_DATA(string _BUID)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("DSH_USP_MAP_SALES_DATA", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@ExtID", _BUID);

				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}

		public DataTable BUNIT_DETAILS(string _BUID)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("USP_BUNIT_DETAILS", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@ORG", _BUID);

				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}

		public DataTable DIST_LISTS(int _DivID, int OrgType)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("USP_DISTRICT_OUTLET_LIST", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@DIVISIONID", _DivID);
				cmd.Parameters.AddWithValue("@ORG_TYPE", OrgType);

				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}

		public DataTable OUTLET_LISTS(int _DistID, int OrgType)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				string _sql = " SELECT * FROM OUTLET_LOC WHERE DISTRICT = '" + _DistID + "' and ORG_TYPE = '" + OrgType + "' AND ACTIVE = 1";

				SqlCommand cmd = new SqlCommand(_sql, conn);
				cmd.CommandType = System.Data.CommandType.Text;
				cmd.CommandTimeout = 500;

				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}

		public DataTable OUTLET_RSALES_CAT(string _BUID)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("DSH_USP_MAP_MULTIPLE_OUTLET_RSALES_CAT", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@ShopIDs", _BUID);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}

		public DataSet OUTLET_ASSET_RENT_INFO(string _BUID)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataSet dt = new DataSet();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("SP_OUTLET_ASSET_RENT_INFO", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@OUTLET_EXT", _BUID);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}

		public DataTable TotalSales()

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				string _sql = "SELECT * INTO #tmpSorted FROM OPENROWSET('SQLNCLI','Server=192.168.3.7,1999;uid=sa;pwd=sa9;', 'EXEC POSXR.[dbo].RPT_ANL_GRAPH_RAMADAN_COMPARE') SELECT * FROM #tmpSorted t WHERE t.RamadanDay = (select DaySL from FN_CAL_RAMADAN(2022) WHERE theDate = convert(varchar, getdate(), 23)) DROP table #tmpSorted";

				SqlCommand cmd = new SqlCommand(_sql, conn);
				cmd.CommandType = System.Data.CommandType.Text;
				cmd.CommandTimeout = 500;

				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}

		public DataTable BATA_APEX_INFO(int type, string _district)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}

				string _sql = "";

				if (type == 1)
				{
					_sql = "SELECT ADDRESS, AREA FROM BATA_LOC BL LEFT JOIN DISTRICTS D ON D.DISTRICTSID = BL.DIST_ID WHERE D.NAME = '" + _district + "' ";


				}

				if (type == 2)
				{
					_sql = "SELECT ADDRESS, AREA FROM APEX_LOC BL LEFT JOIN DISTRICTS D ON D.DISTRICTSID = BL.DIST_ID WHERE D.NAME = '" + _district + "'";


				}

				SqlCommand cmd = new SqlCommand(_sql, conn);
				cmd.CommandType = System.Data.CommandType.Text;
				cmd.CommandTimeout = 500;

				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}
		public DataTable Rent_Details_Info(string _BUID)
		{
			DataTable dt = new DataTable();
			if (conn.State == 0)
			{
				conn.Open();
			}
			try
			{
				SqlCommand cmd = new SqlCommand("SP_OUTLET_RENT_DETAILS_INFO", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.Parameters.AddWithValue("@OUTLET", _BUID);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex;
			}

		}

		public DataTable Factory_Productuon_Info(string _Type)
		{
			DataTable dt = new DataTable();
			if (conn.State == 0)
			{
				conn.Open();
			}
			try
			{
				SqlCommand cmd = new SqlCommand("DSH_USP_MAP_FACT_CAT_PROD", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.Parameters.AddWithValue("@RptType", _Type);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex;
			}

		}
	}
}