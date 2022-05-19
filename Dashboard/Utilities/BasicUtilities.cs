using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;

namespace Dashboard.Utilities
{
	public class BasicUtilities
	{
		public List<Dictionary<string, object>> GetTableRows(DataTable dtData)
		{
			List<Dictionary<string, object>> lstRows = new List<Dictionary<string, object>>();
			Dictionary<string, object> dictRow = null;

			foreach (DataRow dr in dtData.Rows)
			{
				dictRow = new Dictionary<string, object>();
				foreach (DataColumn col in dtData.Columns)
				{
					dictRow.Add(col.ColumnName, dr[col]);
				}
				lstRows.Add(dictRow);
			}
			return lstRows;
		}
	}
}