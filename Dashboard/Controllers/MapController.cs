using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Data;
using Dashboard.DAL;
using Dashboard.Utilities;
using System.Threading.Tasks;

namespace Dashboard.Controllers
{
	public class MapController : Controller
	{
		MapDAL mapDAL = new MapDAL();

		BasicUtilities basicUtilities = new BasicUtilities();

		public ActionResult Index()
		{
			Session["Selected_OutletType"] = 100;
			ViewBag.Selected_OutletType = 100;
			return View();
		}

		public ActionResult Map()
		{
			return View();
		}
		[HttpPost]
		public JsonResult MAP_SALES_DATA(string _BUID)
		{
			try
			{
				if (_BUID != "")
				{
					DataTable dt = mapDAL.MAP_SALES_DATA(_BUID);
					List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

					return Json(_List);
				}

				return Json(false);
			}
			catch (Exception ex)
			{
				return Json(false);
			}
		}

		[HttpPost]
		public JsonResult OUTLET_RSALES_CAT(string _BUID)
		{
			try
			{
				DataTable dt = mapDAL.OUTLET_RSALES_CAT(_BUID);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult OUTLET_ASSET_RENT_INFO(string _BUID)
		{
			try
			{
				DataSet dt = mapDAL.OUTLET_ASSET_RENT_INFO(_BUID);
				List<Dictionary<string, object>> _List1 = basicUtilities.GetTableRows(dt.Tables[0]);
				List<Dictionary<string, object>> _List2 = basicUtilities.GetTableRows(dt.Tables[1]);
				List<Dictionary<string, object>> _List3 = basicUtilities.GetTableRows(dt.Tables[2]);
				List<Dictionary<string, object>> _List4 = basicUtilities.GetTableRows(dt.Tables[3]);
				return Json(new { data1 = _List1, data2 = _List2, data3 = _List3, data4 = _List4 }, JsonRequestBehavior.AllowGet);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult BUNIT_DETAILS(string _BUID)
		{
			try
			{
				DataTable dt = mapDAL.BUNIT_DETAILS(_BUID);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult DIST_LISTS(string _DIVID, int _ORGTYPE)
		{
			try
			{
				int DivId = Convert.ToInt32(_DIVID);
				_ORGTYPE = Convert.ToInt32(Session["Selected_OutletType"].ToString());
				DataTable dt = mapDAL.DIST_LISTS(DivId, _ORGTYPE);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult OUTLET_LISTS(string _DISTID)
		{
			try
			{
				int DistId = Convert.ToInt32(_DISTID);
				int _ORGTYPE = Convert.ToInt32(Session["Selected_OutletType"].ToString());
				DataTable dt = mapDAL.OUTLET_LISTS(DistId, _ORGTYPE);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult Selected_Org_Type(int _MapType)
		{
			try
			{

				Session["Selected_OutletType"] = _MapType;
				DataTable dt = new DataTable();
				dt.Columns.Add("MapType", typeof(string));
				dt.Rows.Add(_MapType);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_MapType);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult GetSelected_Org_Type()
		{
			try
			{
				int _MapType = 100;
				if (Session["Selected_OutletType"] == null)
				{
					_MapType = 100;
				}
				_MapType = Convert.ToInt32(Session["Selected_OutletType"].ToString());

				return Json(_MapType);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult TotalSales()
		{
			try
			{
				DataTable dt = mapDAL.TotalSales();
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public async Task<JsonResult> BATA_APEX_INFO(string _type, string _district)
		{
			try
			{

				int type = 0;
				if (_type == "Bata")
				{
					type = 1;
				}
				if (_type == "Apex")
				{
					type = 2;
				}
				DataTable dt = await Task.Run(() => mapDAL.BATA_APEX_INFO(type, _district));
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult OutletLists(string _DivisionID, int _MapType, int _FILT_TYPE)
		{
			try
			{
				string _Msg;
				DataTable dt_OutletList = mapDAL.GetOutletDatas(_DivisionID, _MapType, _FILT_TYPE);

				if (dt_OutletList.Rows.Count > 0)
				{
					_Msg = "DONE";
					List<Dictionary<string, object>> _OutletList = basicUtilities.GetTableRows(dt_OutletList);
					return Json(_OutletList);
				}
				else
				{
					_Msg = "ERROR";
					return Json(_Msg);
				}
			}
			catch (Exception ex)
			{
				return Json(ex.Message);

			}
		}

		[HttpPost]
		public JsonResult sideInfo(string _divsion, int _type)
		{
			try
			{
				DataTable dt = mapDAL.sideInfo(_divsion, _type);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult Rent_Details_Info(string _BUID)
		{
			try
			{
				DataTable dt = mapDAL.Rent_Details_Info(_BUID);
				List<Dictionary<string, object>> _List = basicUtilities.GetTableRows(dt);

				return Json(_List);
			}
			catch (Exception ex)
			{
				return Json(ex.Message);
			}
		}

		[HttpPost]
		public JsonResult Factory_Productuon_Info(string _Type)
		{
			try
			{
				DataTable dt = mapDAL.Factory_Productuon_Info(_Type);
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