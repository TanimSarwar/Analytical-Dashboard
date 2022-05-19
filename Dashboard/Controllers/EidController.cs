using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Dashboard.DAL;
using Dashboard.Utilities;

using System.Data;

namespace Dashboard.Controllers
{
    public class EidController : Controller
    {
		ChartDAL chartDAL = new ChartDAL();
		BasicUtilities basicUtilities = new BasicUtilities();
		// GET: Eid
		public ActionResult Index()
        {
            return View();
        }

		[HttpPost]
		public JsonResult GetDayWiseEidSalesData()
		{
			try
			{
				DataTable dt = chartDAL.GetDayWiseEidSalesData();
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult EID_TOP_ART(string _cat, string _top, string _date)
		{
			try
			{
				int cat = Convert.ToInt32(_cat);
				if(cat == null)
				{
					cat = 0;
				}
				DataTable dt = chartDAL.EID_TOP_ART(cat, _top, _date);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public ActionResult Get_Cat_Margin_PieGraph(string _Type)
		{
			try
			{
				DataTable dt_CatMarginGraph = chartDAL.EID_CAT_WISE_COMPARISON(_Type);
				List<Dictionary<string, object>> dt_CatMarginGraph_List = basicUtilities.GetTableRows(dt_CatMarginGraph);
				return Json(dt_CatMarginGraph_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}
	}
}