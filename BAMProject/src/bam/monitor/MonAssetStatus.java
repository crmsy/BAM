package bam.monitor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.faw_qm.cp.bam.webservice.util.DataObjectOperationByIDUtil;

import bam.jdbc.JDBCUtil;
import bam.monitor.control.MonCtrl;
import bam.util.CommonUtil;

public class MonAssetStatus implements MonCtrl {
	protected static Connection con;

	public void getDataInfo() {
		// TODO Auto-generated method stub
		getSubAssetCountInfo();
		getAssetBrandInfo();
		getSubTypeInfo();
		getProdCountInfo();
	}
	/**
	 *取子公司的车辆数量
	 */
	private void getSubAssetCountInfo()
	{
		String dateStr = CommonUtil.getCurDateStr();
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT s_bu.name BU_NAME,to_char(s_asset.last_upd,'YYYYMMDD') OPERATEDATE,COUNT(*) ASSET_COUNT FROM siebel.s_asset s_asset  INNER JOIN siebel.s_prod_int s_prod_int ON s_asset.prod_id = s_prod_int.row_id  INNER JOIN siebel.s_bu ON s_prod_int.bu_id = s_bu.row_id   WHERE s_asset.Last_Upd < to_date('");
		sb.append(dateStr);
		sb.append("','YYYYMMDD') group by s_bu.name,to_char(s_asset.last_upd,'YYYYMMDD') ORDER BY to_char(s_asset.last_upd,'YYYYMMDD') DESC ");
		PreparedStatement pm = null;
		ResultSet set = null;
		List dataList = new ArrayList();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(JDBCUtil.urls, JDBCUtil.username, JDBCUtil.passward);
			pm = con.prepareStatement(sb.toString());
			set = pm.executeQuery();
			
			while(set.next())
			{
				Object[] objs = new Object[3];
				String subcomName = set.getString("BU_NAME");
				String dateString = set.getString("OPERATEDATE");
				int count = set.getInt("ASSET_COUNT");
				objs[0] = subcomName;
				objs[1] = dateString;
				objs[2] = new Integer(count);
				dataList.add(objs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (set != null) {
					set.close();
				}
				if (pm != null) {
					pm.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			closeConnection();
		}
		// 删除方法
		String dataObject1 = "/CRM/asset/MonSubAssetCountStatus";
		String keysCSV1 = "_keyfield";
		String xmlPayload1 = "<_MonSubAssetCountStatus><_keyfield>CRM</_keyfield></_MonSubAssetCountStatus>";
		DataObjectOperationByIDUtil.delete(dataObject1, keysCSV1, xmlPayload1);
		int size = dataList.size();
		if(size==0)
			return;
		if(size>19)
			size=19;
		for(int i=0;i<size;i++)
		{
			Object[] objs = (Object[])dataList.get(i);
			String subcomName = (String)objs[0];
			String dateString = (String)objs[1];
			int count = ((Integer)objs[2]).intValue();
			String[] paras = { "BU_NAME#"+subcomName, 
					"ASSET_COUNT#"+count,
					"DAY#"+dateString,
					"keyfield#CRM"};
			DataObjectOperationByIDUtil.insert("/CRM/asset/MonSubAssetCountStatus", paras);
		}
	}
	
	/**
	 * 取品牌的信息
	 */
	private void getAssetBrandInfo()
	{
		String dateStr = CommonUtil.getCurDateStr();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT S_CTLG_CAT_BRAND.Display_Name BRAND_NAME,to_char(S_ASSET.Last_Upd,'YYYYMMDD'),COUNT(S_ASSET.ASSET_NUM) BRAND_ASSET_COUNT FROM siebel.s_prod_int s_prod_int ");
		sb.append(" INNER JOIN SIEBEL.S_ASSET S_ASSET ON S_ASSET.PROD_ID = s_prod_int.Row_Id ");
		sb.append(" INNER JOIN siebel.S_CTLG_CAT S_CTLG_CAT_AUTO_SUB_TYPE ON S_PROD_INT.CG_PR_CTLG_CAT_ID = S_CTLG_CAT_AUTO_SUB_TYPE.ROW_ID ");
		sb.append(" INNER JOIN siebel.S_CTLG_CAT S_CTLG_CAT_AUTO_TYPE ON S_CTLG_CAT_AUTO_SUB_TYPE.Par_Cat_Id = S_CTLG_CAT_AUTO_TYPE.Row_Id ");
		sb.append(" INNER JOIN siebel.S_CTLG_CAT S_CTLG_CAT_BRAND ON S_CTLG_CAT_AUTO_TYPE.Par_Cat_Id = S_CTLG_CAT_BRAND.Row_Id ");
		sb.append(" WHERE S_CTLG_CAT_BRAND.CG_LEVEL_CD = 'Brand' and S_CTLG_CAT_BRAND.Display_Name in ('奔腾','森雅','夏利','威姿') GROUP BY S_CTLG_CAT_BRAND.Display_Name ,to_char(S_ASSET.Last_Upd,'YYYYMMDD') order by to_char(S_ASSET.Last_Upd,'YYYYMMDD') desc");
		PreparedStatement pm = null;
		ResultSet set = null;
		List dataList = new ArrayList();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(JDBCUtil.urls, JDBCUtil.username, JDBCUtil.passward);
			pm = con.prepareStatement(sb.toString());
			set = pm.executeQuery();
			
			while(set.next())
			{
				Object[] objs = new Object[3];
				String brandName = set.getString(1);
				String dateString = set.getString(2);
				int count = set.getInt(3);
				objs[0] = brandName;
				objs[1] = dateString;
				objs[2] = new Integer(count);
				dataList.add(objs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (set != null) {
					set.close();
				}
				if (pm != null) {
					pm.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			closeConnection();
		}
		// 删除方法
		String dataObject1 = "/CRM/asset/MonBrandCount";
		String keysCSV1 = "_keyfield";
		String xmlPayload1 = "<_MonBrandCount><_keyfield>CRM</_keyfield></_MonBrandCount>";
		DataObjectOperationByIDUtil.delete(dataObject1, keysCSV1, xmlPayload1);
		int size = dataList.size();
		if(size==0)
			return;
		if(size>19)
			size=19;
		for(int i=0;i<size;i++)
		{
			Object[] objs = (Object[])dataList.get(i);
			String brandName = (String)objs[0];
			String dateString = (String)objs[1];
			int count = ((Integer)objs[2]).intValue();
			String[] paras = { "BRAND_NAME#"+brandName, 
					"BRAND_ASSET_COUNT#"+count,
					"BRAND_DAY#"+dateString,
					"keyfield#CRM"};
			DataObjectOperationByIDUtil.insert("/CRM/asset/MonBrandCount", paras);
		}
	}
	/**
	 * 取车型的信息
	 */
	private void getSubTypeInfo()
	{
		String dateStr = CommonUtil.getCurDateStr();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT s_bu.name BU_NAME,COUNT(distinct S_CTLG_CAT_AUTO_SUB_TYPE.DISPLAY_NAME) AUTO_SUB_TYPE_COUNT  ");
		sb.append(" FROM siebel.s_prod_int s_prod_int ");
		sb.append(" INNER JOIN siebel.S_CTLG_CAT S_CTLG_CAT_AUTO_SUB_TYPE ON S_PROD_INT.CG_PR_CTLG_CAT_ID = S_CTLG_CAT_AUTO_SUB_TYPE.ROW_ID ");
		sb.append(" INNER JOIN siebel.s_bu s_bu ON s_prod_int.bu_id = s_bu.row_id ");
		sb.append(" WHERE S_CTLG_CAT_AUTO_SUB_TYPE.CG_LEVEL_CD = 'Auto Sub Type' ");
		sb.append(" GROUP BY s_bu.name ");
		PreparedStatement pm = null;
		ResultSet set = null;
		List dataList = new ArrayList();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(JDBCUtil.urls, JDBCUtil.username, JDBCUtil.passward);
			pm = con.prepareStatement(sb.toString());
			set = pm.executeQuery();
			
			while(set.next())
			{
				Object[] objs = new Object[3];
				String buName = set.getString(1);
				int count = set.getInt(2);
				objs[0] = buName;
				objs[1] = new Integer(count);
				dataList.add(objs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (set != null) {
					set.close();
				}
				if (pm != null) {
					pm.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			closeConnection();
		}
		// 删除方法
		String dataObject1 = "/CRM/asset/MonSubAutoSubTypeCountStatus";
		String keysCSV1 = "_keyfield";
		String xmlPayload1 = "<_MonSubAutoSubTypeCountStatus><_keyfield>CRM</_keyfield></_MonSubAutoSubTypeCountStatus>";
		DataObjectOperationByIDUtil.delete(dataObject1, keysCSV1, xmlPayload1);
		if(dataList.size()==0)
			return;
		for(int i=0;i<dataList.size();i++)
		{
			Object[] objs = (Object[])dataList.get(i);
			String buName = (String)objs[0];
			int count = ((Integer)objs[1]).intValue();
			String[] paras = { "BU_NAME#"+buName, 
					"AUTO_SUB_TYPE_COUNT#"+count,
					"keyfield#CRM"};
			DataObjectOperationByIDUtil.insert("/CRM/asset/MonSubAutoSubTypeCountStatus", paras);
		}
	}
	/**
	 * 统计产品的信息
	 */
	private void getProdCountInfo()
	{
		String dateStr = CommonUtil.getCurDateStr();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT s_bu.name BU_NAME,count(s_prod_int.name) PROD_COUNT ");
		sb.append(" FROM siebel.s_prod_int s_prod_int ");
		sb.append(" INNER JOIN siebel.s_bu s_bu ON s_prod_int.bu_id = s_bu.row_id");
		sb.append(" GROUP BY s_bu.name");
		PreparedStatement pm = null;
		ResultSet set = null;
		List dataList = new ArrayList();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(JDBCUtil.urls, JDBCUtil.username, JDBCUtil.passward);
			pm = con.prepareStatement(sb.toString());
			set = pm.executeQuery();
			
			while(set.next())
			{
				Object[] objs = new Object[3];
				String buName = set.getString(1);
				int count = set.getInt(2);
				objs[0] = buName;
				objs[1] = new Integer(count);
				dataList.add(objs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (set != null) {
					set.close();
				}
				if (pm != null) {
					pm.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			closeConnection();
		}
		// 删除方法
		String dataObject1 = "/CRM/asset/MonSubProdCountStatus";
		String keysCSV1 = "_keyfield";
		String xmlPayload1 = "<_MonSubProdCountStatus><_keyfield>CRM</_keyfield></_MonSubProdCountStatus>";
		DataObjectOperationByIDUtil.delete(dataObject1, keysCSV1, xmlPayload1);
		if(dataList.size()==0)
			return;
		for(int i=0;i<dataList.size();i++)
		{
			Object[] objs = (Object[])dataList.get(i);
			String buName = (String)objs[0];
			int count = ((Integer)objs[1]).intValue();
			String[] paras = { "BU_NAME#"+buName, 
					"PROD_COUNT#"+count,
					"keyfield#CRM"};
			DataObjectOperationByIDUtil.insert("/CRM/asset/MonSubProdCountStatus", paras);
		}
	}
	private void closeConnection() {
		try {
			if (con != null && !con.isClosed()) {
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}