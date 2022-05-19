using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;
using Dashboard.Utilities;


namespace Dashboard.DAL
{
	public class ChartDAL
	{
		public static SqlConnection conn = new SqlConnection(DBConnection.GetConnectionString());
		public static SqlConnection conn1 = new SqlConnection(DBConnection.GetConnectionString());
		public static SqlConnection conn2 = new SqlConnection(DBConnection.GetConnectionString2());
		public DataTable GetDayWiseSalesData(string _startDate, string _endDate, int _trend)
		{
			try
			{
				if (conn.State == 0)
				{
					conn.Open();
				}
				DataTable dt = new DataTable();
				SqlCommand cmd = new SqlCommand("RPT_RMARGIN", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.Parameters.AddWithValue("@StartDate", _startDate);
				cmd.Parameters.AddWithValue("@EndDate", _endDate);
				cmd.Parameters.AddWithValue("@Type", _trend);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex;
			}
		}

		public DataTable RPT_RMARGIN()
		{
			try
			{
				if (conn.State == 0)
				{
					conn.Open();
				}
				DataTable dt = new DataTable();
				SqlCommand cmd = new SqlCommand("RPT_RMARGIN", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				string StartDate = DateTime.Today.AddDays(-6).ToString("MM-dd-yyyy");
				string EndDate = DateTime.Today.ToString("MM-dd-yyyy");
				cmd.Parameters.AddWithValue("@StartDate", StartDate);
				cmd.Parameters.AddWithValue("@EndDate", EndDate);
				cmd.Parameters.AddWithValue("@Type", 1);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex;
			}
		}

		public DataTable GetHourlySalesData()
		{
			try
			{
				if (conn.State == 0)
				{
					conn.Open();
				}
				DataTable dt = new DataTable();
				SqlCommand cmd = new SqlCommand("RPT_RMARGIN_HOUR", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				string yesterDayDate = DateTime.Today.AddDays(-1).ToString("MM-dd-yyyy");
				string TodayDate = DateTime.Today.ToString("MM-dd-yyyy");
				cmd.Parameters.AddWithValue("@StartDt", yesterDayDate);
				cmd.Parameters.AddWithValue("@EndDt", TodayDate);
				cmd.Parameters.AddWithValue("@Type", 3);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex;
			}
		}




		public DataTable GetOutletData(int _type)
		{
			try
			{
				if (conn.State == 0)
				{
					conn.Open();
				}
				string _sql = "SELECT EXTERNALID + ':' + ORG_NAME [Outlet_Name], * FROM OUTLET_LOC WHERE Lattitude <> 0 AND ORG_TYPE = '"+_type+"'";
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
				string _sql="";
				if(_type == 1)
				{
					 _sql = "SELECT D.NAME,D.SHORT_KEY ,DI.[AREA],  DI.POPULATION,MALE_PER,FEMALE_PER FROM DIVISIONS_INFO DI LEFT JOIN DIVISIONS D ON D.DIVISIONSID = DI.DIVISION_ID WHERE D.SHORT_KEY = '" + _SHORT_KEY + "'";

				}
				if (_type == 2)
				{
					 _sql = "SELECT * FROM DISTRICTS_INFO DI LEFT JOIN DISTRICTS D ON D.DISTRICTSID = DI.DISTRICTS_ID WHERE NAME = '"+_SHORT_KEY+"'";

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


		public DataTable Get_Chnl_Margin_DonutGraph(string _FrmDate, string _ToDate)
		{
			DataTable dt = new DataTable();
			if (conn.State == 0)
			{
				conn.Open();
			}
			try
			{
				SqlCommand cmd = new SqlCommand("RPT_CHNL_SALES_GRAPH", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.Parameters.AddWithValue("@StartDt", _FrmDate);
				cmd.Parameters.AddWithValue("@EndDt", _ToDate);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex;
			}

		}
		public DataTable Get_Cat_Margin_Graph(string _FrmDate, string _ToDate)
		{
			DataTable dt = new DataTable();
			if (conn.State == 0)
			{
				conn.Open();
			}

			try
			{
				SqlCommand cmd = new SqlCommand("RPT_CAT_MARGIN_GRAPH", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.Parameters.AddWithValue("@StartDt", _FrmDate);
				cmd.Parameters.AddWithValue("@EndDt", _ToDate);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex;
			}



		}

		public DataTable RPT_DSH_WKDAY(string _FrmDate, string _ToDate, string _TimeSlot, string _WeekDay, string _ReportType)
		{
			DataTable dt = new DataTable();
			if (conn.State == 0)
			{
				conn.Open();
			}
			try
			{
				SqlCommand cmd = new SqlCommand("USP_RPT_DSH_WKDAY", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.Parameters.AddWithValue("@StartDT", _FrmDate);
				cmd.Parameters.AddWithValue("@EndDT", _ToDate);
				cmd.Parameters.AddWithValue("@WK", _WeekDay);
				cmd.Parameters.AddWithValue("@hourname", _TimeSlot);
				cmd.Parameters.AddWithValue("@RPTTYPE", _ReportType);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex;
			}
		}

		public DataTable RPT_DSH_Hour_Details(string _st_date, string _et_date, string _rpt_type)

		{
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("USP_RPT_DSH_HOURLY", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@StartDT", _st_date);
				cmd.Parameters.AddWithValue("@EndDT", _et_date);
				cmd.Parameters.AddWithValue("@Grd", '0');
				cmd.Parameters.AddWithValue("@Zone", '0');
				cmd.Parameters.AddWithValue("@Area", '0');
				cmd.Parameters.AddWithValue("@Org", '0');
				cmd.Parameters.AddWithValue("@RPTYPE", _rpt_type);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}

		public DataTable RPT_TOP_ARTICLES(string _st_date, string _et_date, string _Type, string _Cat)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("SP_TOP_TOPARTICLES", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@s_date", _st_date);
				cmd.Parameters.AddWithValue("@e_date", _et_date);
				cmd.Parameters.AddWithValue("@grd", '0');
				cmd.Parameters.AddWithValue("@zone", '0');
				cmd.Parameters.AddWithValue("@area", '0');
				cmd.Parameters.AddWithValue("@org", '0');
				cmd.Parameters.AddWithValue("@cat", '0');
				cmd.Parameters.AddWithValue("@scat", '0');
				cmd.Parameters.AddWithValue("@seg", '0');
				cmd.Parameters.AddWithValue("@bnd", '0');
				cmd.Parameters.AddWithValue("@ses", '0');
				cmd.Parameters.AddWithValue("@gen", '0');
				cmd.Parameters.AddWithValue("@supp", '0');
				cmd.Parameters.AddWithValue("@art", '0');
				cmd.Parameters.AddWithValue("@color", '0');
				cmd.Parameters.AddWithValue("@size", '0');
				cmd.Parameters.AddWithValue("@nos", '0');
				cmd.Parameters.AddWithValue("@bsale", '0');
				cmd.Parameters.AddWithValue("@t", 10);
				cmd.Parameters.AddWithValue("@ord", Convert.ToInt32(_Type));
				cmd.Parameters.AddWithValue("@Category", _Cat);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}


		public DataTable RPT_TOP_SALESMAN(string _st_date, string _et_date, string _Top, string _Area)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("RPT_SALESFORCE_TOP", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@EndDt", _st_date);
				cmd.Parameters.AddWithValue("@StartDt", _et_date);
				cmd.Parameters.AddWithValue("@TOP", _Top);
				cmd.Parameters.AddWithValue("@AREA", _Area);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}



		
		public DataTable RPT_TARGETACIEVE(string _st_date, string _et_date)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("DSH_USP_RETAIL_TARGET_ACTUAL", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@EndDT", _st_date);
				cmd.Parameters.AddWithValue("@StartDT", _et_date);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}


		public DataTable TOPBOTTOMOUTLET(string _ToDate, string _FrmDate, string _type, string _sort)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("DSH_USP_RETAIL_DAY_LOCATION", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@StartDT", _ToDate);
				cmd.Parameters.AddWithValue("@EndDT", _FrmDate);
				cmd.Parameters.AddWithValue("@SortType", _sort);
				cmd.Parameters.AddWithValue("@Location", _type);

				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
			}
		}

		////////////////////////////////////////////
		public DataTable TOP_ART_EMP(string _topType, string _top, string _date)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("DSH_USP_RETAIL_TOP_ART_EMP", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@topType", _topType);
				cmd.Parameters.AddWithValue("@topCount", _top);
				cmd.Parameters.AddWithValue("@SaleDT", _date);

				cmd.Parameters.AddWithValue("@grd", 0);
				cmd.Parameters.AddWithValue("@zone", 0);
				cmd.Parameters.AddWithValue("@area", 0);
				cmd.Parameters.AddWithValue("@org", 0);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex.InnerException;
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



		public DataTable RPT_COMPARE_TY_LY(string _sort, string _type, string _date)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("DSH_USP_RETAIL_DAY_LOCATION_COMPARE", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@StartDate", "04/01/2022");
				cmd.Parameters.AddWithValue("@SortType", _sort);
				cmd.Parameters.AddWithValue("@Location", _type);

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
		///////////////////////////////
		///
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
				string	_sql = " SELECT * FROM OUTLET_LOC WHERE DISTRICT = '"+_DistID+"' and ORG_TYPE = '"+OrgType+"' AND ACTIVE = 1";

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
		/// 
		///

		public DataTable TOP_DO_MAKER(int type)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn2.State == 0)
				{
					conn2.Open();
				}

				string _sql = "";

				if (type == 1)
				{
					_sql = "SELECT DISTINCT(SO.Created_By), SUM(Net_Amount)SUM, COUNT(External_ID)[No of DO], NAME FROM SALES_ORDER_HEADER SO INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-24','2022-03-24') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON SO.Order_No=b.TRNREQNO LEFT JOIN EMPLOYEE_MASTER EMP ON SO.Created_By = EMP.EMP_ID LEFT JOIN PARTNER_MASTER PM ON PM .PInt_ID = SO.Customer_ID WHERE SO.Created_By <> 7500019 AND PM.ChID = 102 GROUP BY SO.Created_By, NAME ORDER BY SUM DESC";


				}

				if (type == 2)
				{
					_sql = "SELECT DISTINCT(SO.Created_By), SUM(Net_Amount)SUM, COUNT(External_ID)[No of DO], NAME FROM SALES_ORDER_HEADER SO INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-27','2022-03-27') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON SO.Order_No=b.TRNREQNO LEFT JOIN EMPLOYEE_MASTER EMP ON SO.Created_By = EMP.EMP_ID LEFT JOIN PARTNER_MASTER PM ON PM .PInt_ID = SO.Customer_ID WHERE SO.Created_By <> 7500019 AND PM.ChID = 101 GROUP BY SO.Created_By, NAME ORDER BY SUM DESC";


				}

				if (type == 3)
				{
					_sql = "SELECT DISTINCT(SO.Created_By), SUM(Net_Amount)SUM, COUNT(External_ID)[No of DO], NAME FROM SALES_ORDER_HEADER SO INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-28','2022-03-28') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON SO.Order_No=b.TRNREQNO LEFT JOIN EMPLOYEE_MASTER EMP ON SO.Created_By = EMP.EMP_ID LEFT JOIN PARTNER_MASTER PM ON PM .PInt_ID = SO.Customer_ID WHERE SO.Created_By <> 7500019 AND PM.ChID = 101 GROUP BY SO.Created_By, NAME ORDER BY SUM DESC";




				}

				if (type == 4)
				{
					_sql = "SELECT DISTINCT(SO.Created_By), SUM(Net_Amount)SUM, COUNT(External_ID)[No of DO], NAME FROM SALES_ORDER_HEADER SO INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-29','2022-03-29') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON SO.Order_No=b.TRNREQNO LEFT JOIN EMPLOYEE_MASTER EMP ON SO.Created_By = EMP.EMP_ID LEFT JOIN PARTNER_MASTER PM ON PM .PInt_ID = SO.Customer_ID WHERE SO.Created_By <> 7500019 AND PM.ChID = 101 GROUP BY SO.Created_By, NAME ORDER BY SUM DESC";




				}



				SqlCommand cmd = new SqlCommand(_sql, conn2);
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


		


		public DataTable WHSALES( int type)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn2.State == 0)
				{
					conn2.Open();
				}
				string _sql = "";
				if (type == 1)
				{
					_sql = "SELECT count(distinct Customer_ID) CustNo, COUNT(Order_No)OrderNo, SUM(Net_Amount)NetAmt FROM SALES_ORDER_HEADER SO INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-24','2022-03-24') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON SO.Order_No=b.TRNREQNO LEFT JOIN PARTNER_MASTER PM ON PM .PInt_ID = SO.Customer_ID WHERE SO.Created_By <> 7500019 AND PM.ChID = 102";


				}

				if (type == 2)
				{
					_sql = "SELECT count(distinct Customer_ID) CustNo, COUNT(Order_No)OrderNo, SUM(Net_Amount)NetAmt FROM SALES_ORDER_HEADER SO INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-27','2022-03-27') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON SO.Order_No=b.TRNREQNO LEFT JOIN PARTNER_MASTER PM ON PM .PInt_ID = SO.Customer_ID WHERE SO.Created_By <> 7500019 AND PM.ChID = 101";


				}

				if (type == 3)
				{
					_sql = "SELECT count(distinct Customer_ID) CustNo, COUNT(Order_No)OrderNo, SUM(Net_Amount)NetAmt FROM SALES_ORDER_HEADER SO INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-28','2022-03-28') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON SO.Order_No=b.TRNREQNO LEFT JOIN PARTNER_MASTER PM ON PM .PInt_ID = SO.Customer_ID WHERE SO.Created_By <> 7500019 AND PM.ChID = 101";



				}

				if (type == 4)
				{
					_sql = "SELECT count(distinct Customer_ID) CustNo, COUNT(Order_No)OrderNo, SUM(Net_Amount)NetAmt FROM SALES_ORDER_HEADER SO INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-29','2022-03-29') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON SO.Order_No=b.TRNREQNO LEFT JOIN PARTNER_MASTER PM ON PM .PInt_ID = SO.Customer_ID WHERE SO.Created_By <> 7500019 AND PM.ChID = 101";



				}

				SqlCommand cmd = new SqlCommand(_sql, conn2);
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

		public DataTable TOP_WH_BUYER(int type)

		{
			try
			{
				DataTable dt = new DataTable();
				if (conn2.State == 0)
				{
					conn2.Open();
				}
				string _sql = "";
				if(type == 1)
				{
					_sql = "SELECT TOP 30 * FROM ( SELECT DISTINCT(PM.PartnerName+': '+PM.Partner_ID)PartnerName, SUM(SO.Net_Amount)SUM, COUNT(External_ID)[NoofDO] FROM SALES_ORDER_HEADER SO INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-24','2022-03-24') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON SO.Order_No=b.TRNREQNO LEFT JOIN EMPLOYEE_MASTER EMP ON SO.Created_By = EMP.EMP_ID LEFT JOIN PARTNER_MASTER PM ON PM .PInt_ID = SO.Customer_ID WHERE SO.Created_By <> 7500019 AND PM.ChID = 102 GROUP BY PM.PartnerName, NAME, Partner_ID )A ORDER BY SUM DESC";

				}

				if (type == 2)
				{
					_sql = "SELECT TOP 30 * FROM ( SELECT DISTINCT(PM.PartnerName+': '+PM.Partner_ID)PartnerName, SUM(SO.Net_Amount)SUM, COUNT(External_ID)[NoofDO] FROM SALES_ORDER_HEADER SO INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-27','2022-03-27') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON SO.Order_No=b.TRNREQNO LEFT JOIN EMPLOYEE_MASTER EMP ON SO.Created_By = EMP.EMP_ID LEFT JOIN PARTNER_MASTER PM ON PM .PInt_ID = SO.Customer_ID WHERE SO.Created_By <> 7500019 AND PM.ChID = 101 GROUP BY PM.PartnerName, NAME, Partner_ID )A ORDER BY SUM DESC";


				}

				if (type == 3)
				{
					_sql = "SELECT TOP 30 * FROM ( SELECT DISTINCT(PM.PartnerName+': '+PM.Partner_ID)PartnerName, SUM(SO.Net_Amount)SUM, COUNT(External_ID)[NoofDO] FROM SALES_ORDER_HEADER SO INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-28','2022-03-28') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON SO.Order_No=b.TRNREQNO LEFT JOIN EMPLOYEE_MASTER EMP ON SO.Created_By = EMP.EMP_ID LEFT JOIN PARTNER_MASTER PM ON PM .PInt_ID = SO.Customer_ID WHERE SO.Created_By <> 7500019 AND PM.ChID = 101 GROUP BY PM.PartnerName, NAME, Partner_ID )A ORDER BY SUM DESC";


				}

				if (type == 4)
				{
					_sql = "SELECT TOP 30 * FROM ( SELECT DISTINCT(PM.PartnerName+': '+PM.Partner_ID)PartnerName, SUM(SO.Net_Amount)SUM, COUNT(External_ID)[NoofDO] FROM SALES_ORDER_HEADER SO INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-29','2022-03-29') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON SO.Order_No=b.TRNREQNO LEFT JOIN EMPLOYEE_MASTER EMP ON SO.Created_By = EMP.EMP_ID LEFT JOIN PARTNER_MASTER PM ON PM .PInt_ID = SO.Customer_ID WHERE SO.Created_By <> 7500019 AND PM.ChID = 101 GROUP BY PM.PartnerName, NAME, Partner_ID )A ORDER BY SUM DESC";


				}
				SqlCommand cmd = new SqlCommand(_sql, conn2);
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




		public DataTable CATPERCENTAGE(int Type)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn2.State == 0)
				{
					conn2.Open();
				}
				string _sql = "";
				if (Type == 1)
				{
					 _sql = "SELECT c.CAT,SUM(a.Net_Amount) NetAmt,SUM(SUM(a.Net_Amount)) OVER() TotalNetAmt,SUM(a.Net_Amount)/SUM(SUM(a.Net_Amount)) OVER()*100.0 [Prcnt] FROM SALES_ORDER_DETAILS a INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-24','2022-03-24') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON a.Order_No=b.TRNREQNO INNER JOIN ( SELECT ART_CODE,b.[Description] [CAT] FROM ARTICLEMASTER a INNER JOIN ARTOLDCAT b ON a.rptop1=b.Cid ) c ON c.ART_CODE=a.article_code INNER JOIN PARTNER_MASTER pm ON pm.PInt_ID=b.TRNCUSID AND pm.ChID=102 GROUP BY c.CAT";
				}

				if (Type == 2)
				{
					 _sql = "SELECT c.CAT,SUM(a.Net_Amount) NetAmt,SUM(SUM(a.Net_Amount)) OVER() TotalNetAmt,SUM(a.Net_Amount)/SUM(SUM(a.Net_Amount)) OVER()*100.0 [Prcnt] FROM SALES_ORDER_DETAILS a INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-27','2022-03-27') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON a.Order_No=b.TRNREQNO INNER JOIN ( SELECT ART_CODE,b.[Description] [CAT] FROM ARTICLEMASTER a INNER JOIN ARTOLDCAT b ON a.rptop1=b.Cid ) c ON c.ART_CODE=a.article_code INNER JOIN PARTNER_MASTER pm ON pm.PInt_ID=b.TRNCUSID AND pm.ChID=101 GROUP BY c.CAT";
				}

				if (Type == 3)
				{
					 _sql = "SELECT c.CAT,SUM(a.Net_Amount) NetAmt,SUM(SUM(a.Net_Amount)) OVER() TotalNetAmt,SUM(a.Net_Amount)/SUM(SUM(a.Net_Amount)) OVER()*100.0 [Prcnt] FROM SALES_ORDER_DETAILS a INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-28','2022-03-28') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON a.Order_No=b.TRNREQNO INNER JOIN ( SELECT ART_CODE,b.[Description] [CAT] FROM ARTICLEMASTER a INNER JOIN ARTOLDCAT b ON a.rptop1=b.Cid ) c ON c.ART_CODE=a.article_code INNER JOIN PARTNER_MASTER pm ON pm.PInt_ID=b.TRNCUSID AND pm.ChID=101 GROUP BY c.CAT";
				}
				if (Type == 4)
				{
					 _sql = "SELECT c.CAT,SUM(a.Net_Amount) NetAmt,SUM(SUM(a.Net_Amount)) OVER() TotalNetAmt,SUM(a.Net_Amount)/SUM(SUM(a.Net_Amount)) OVER()*100.0 [Prcnt] FROM SALES_ORDER_DETAILS a INNER JOIN ( SELECT * FROM FN_SALDLVREF('2022-03-29','2022-03-29') WHERE TRNCUSID!=1816 AND PKSTATUS!=5 ) b ON a.Order_No=b.TRNREQNO INNER JOIN ( SELECT ART_CODE,b.[Description] [CAT] FROM ARTICLEMASTER a INNER JOIN ARTOLDCAT b ON a.rptop1=b.Cid ) c ON c.ART_CODE=a.article_code INNER JOIN PARTNER_MASTER pm ON pm.PInt_ID=b.TRNCUSID AND pm.ChID=101 GROUP BY c.CAT";
				}
				SqlCommand cmd = new SqlCommand(_sql, conn2);
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

		public DataTable GetDayWiseEidSalesData()
		{
			try
			{
				if (conn.State == 0)
				{
					conn.Open();
				}
				DataTable dt = new DataTable();
				SqlCommand cmd = new SqlCommand("RPT_ANL_GRAPH_RAMADAN_COMPARE", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex;
			}
		}



		public DataTable EID_TOP_ART(int _Cat, string _top, string _date)

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn.State == 0)
				{
					conn.Open();
				}
				SqlCommand cmd = new SqlCommand("RPT_EID_TOP_ART", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 500;
				cmd.Parameters.AddWithValue("@CAT", _Cat);
				cmd.Parameters.AddWithValue("@topCount", _top);
				cmd.Parameters.AddWithValue("@SaleDT", _date);
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





		public DataTable EID_CAT_WISE_COMPARISON(string _Type)
		{
			DataTable dt = new DataTable();
			if (conn.State == 0)
			{
				conn.Open();
			}

			try
			{
				SqlCommand cmd = new SqlCommand("EID_CAT_WISE_COMPARISON", conn);
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.Parameters.AddWithValue("@Type", _Type);
				SqlDataAdapter adpt = new SqlDataAdapter(cmd);
				adpt.Fill(dt);
				return dt;
			}
			catch (Exception ex)
			{
				throw ex;
			}



		}
		public DataTable OLDCAT()

		{
			// SqlConnection conns = new SqlConnection("Report_ConnectionString");
			try
			{
				DataTable dt = new DataTable();
				if (conn2.State == 0)
				{
					conn2.Open();
				}
				string _sql = "SELECT * FROM ARTOLDCAT";
				
				SqlCommand cmd = new SqlCommand(_sql, conn2);
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
					_sql = "SELECT ADDRESS, AREA FROM BATA_LOC BL LEFT JOIN DISTRICTS D ON D.DISTRICTSID = BL.DIST_ID WHERE D.NAME = '"+_district+"' ";


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
	}
}
