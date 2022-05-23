using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

using System.Data;

using Dashboard.Utilities;
using Dashboard.DAL;
using System.Threading.Tasks;

namespace Dashboard.Controllers
{
	public class ChartController : Controller
	{
		ChartDAL chartDAL = new ChartDAL();
		BasicUtilities basicUtilities = new BasicUtilities();
		// GET: Chart
		public ActionResult Home()
		{
			return View();
		}

		public ActionResult Chart()
		{
			return View();
		}

		public ActionResult Chartsss()
		{
			return View();
		}

		[HttpPost]
		public JsonResult OLDCAT()
		{
			try
			{
				DataTable dt = chartDAL.OLDCAT();
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult GetDayWiseSalesData(string _startDate, string _endDate)
		{
			try
			{
				DataTable dt = chartDAL.GetDayWiseSalesData(_startDate, _endDate, 2);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult GetHourlySalesData()
		{
			try
			{
				DataTable dt = chartDAL.GetHourlySalesData();
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult RPT_RMARGIN()
		{
			try
			{
				DataTable dt = chartDAL.RPT_RMARGIN();
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult RPT_DSH_WKDAY(string _FrmDate, string _ToDate, string _TimeSlot, string _WeekDay, string _ReportType)
		{
			DataTable dt = chartDAL.RPT_DSH_WKDAY(_FrmDate, _ToDate, _TimeSlot, _WeekDay, _ReportType);
			List<Dictionary<string, object>> _ddllist = basicUtilities.GetTableRows(dt);



			return Json(_ddllist);
		}

		[HttpPost]
		public JsonResult Get_Cat_Margin_DonutGraph()
		{

			try
			{
				string _FromDate = DateTime.Now.ToString("MM/dd/yyyy");
				DateTime _TDate = DateTime.Now.AddDays(-6);
				string _ToDate = _TDate.ToString("MM/dd/yyyy");

				DataTable dt_DonutGraphs = chartDAL.Get_Chnl_Margin_DonutGraph(_ToDate, _FromDate);

				//--------------------------------------
				List<Dictionary<string, object>> dt_DonutGraph_List = basicUtilities.GetTableRows(dt_DonutGraphs);
				return Json(dt_DonutGraph_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public ActionResult Get_Cat_Margin_PieGraph()
		{

			try
			{
				string _FromDate = DateTime.Now.ToString("MM/dd/yyyy");
				DateTime _TDate = DateTime.Now.AddDays(-6);
				string _ToDate = _TDate.ToString("MM/dd/yyyy");

				DataTable dt_CatMarginGraph = chartDAL.Get_Cat_Margin_Graph(_ToDate, _FromDate);

				//--------------------------------------
				List<Dictionary<string, object>> dt_CatMarginGraph_List = basicUtilities.GetTableRows(dt_CatMarginGraph);
				return Json(dt_CatMarginGraph_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult RPT_DSH_Hour(string _st_date, string _et_date, string _rpt_type)
		{
			DataTable dt = chartDAL.RPT_DSH_Hour_Details(_st_date, _et_date, _rpt_type);
			List<Dictionary<string, object>> lists = basicUtilities.GetTableRows(dt);
			return Json(lists);
		}

		[HttpPost]
		public JsonResult RPT_TOP_ARTICLES(string _ToDate, string _FrmDate, string _Type, string _Cat)
		{
			try
			{
				DataTable dt = chartDAL.RPT_TOP_ARTICLES(_FrmDate, _ToDate, _Type, _Cat);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult RPT_TOP_SALESMAN(string _ToDate, string _FrmDate, string _Top, string _Area)
		{
			try
			{
				DataTable dt = chartDAL.RPT_TOP_SALESMAN(_FrmDate, _ToDate, _Top, _Area);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult TOP_ART_EMP(string _topType, string _top, string _date)
		{
			try
			{
				DataTable dt = chartDAL.TOP_ART_EMP(_topType, _top, _date);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult RPT_TARGETACIEVE(string _ToDate, string _FrmDate)
		{
			try
			{
				DataTable dt = chartDAL.RPT_TARGETACIEVE(_FrmDate, _ToDate);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult RPT_TOPOUT(string _ToDate, string _FrmDate, string _type, string _sort)
		{
			try
			{

				DataTable dt = chartDAL.TOPBOTTOMOUTLET(_FrmDate, _ToDate, _type, _sort);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult RPT_BOTTOMOUT(string _ToDate, string _FrmDate, string _type)
		{
			try
			{
				string _sort = "BOTTOM";
				DataTable dt = chartDAL.TOPBOTTOMOUTLET(_FrmDate, _ToDate, _type, _sort);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public async Task<JsonResult> RPT_COMPARE_TY_LY(string _sort, string _type)
		{
			try
			{

				DateTime FIRTSDAY = DateTime.Now.AddDays(-(DateTime.Now.Day - 1));

				var _date = FIRTSDAY.ToString("MM/dd/yyyy");
				DataTable dt = await Task.Run(() => chartDAL.RPT_COMPARE_TY_LY(_sort, _type, _date));
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		public ActionResult WHROADSHOW()
		{
			return View();
		}

		[HttpPost]
		public JsonResult TOP_DO_MAKER(string _Type)
		{
			int _type = Convert.ToInt32(_Type);

			try
			{
				DataTable dt = chartDAL.TOP_DO_MAKER(_type);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult TOP_WH_BUYER(string _Type)
		{
			int _type = Convert.ToInt32(_Type);

			try
			{
				DataTable dt = chartDAL.TOP_WH_BUYER(_type);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult WHSALES(string _Type)
		{
			int _type = Convert.ToInt32(_Type);

			try
			{
				DataTable dt = chartDAL.WHSALES(_type);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult CATPERCENTAGE(string _Type)
		{
			int _type = Convert.ToInt32(_Type);
			try
			{
				DataTable dt = chartDAL.CATPERCENTAGE(_type);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}


	}
}


