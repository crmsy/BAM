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

public class MonCustomerStatus implements MonCtrl {
	protected static Connection con;
	@Override
	public void getDataInfo() {
		// TODO Auto-generated method stub
		//子公司的数量
		getSubCompanyCountInfo();
		//SDH表的数量
		getSDHCountInfo();
		//客户分类
		getAccountTypeInfo();
		//客户类别
		getMarketTypeInfo();
	}
	/**
	 * 客户所属子公司的数量
	 */
	private void getSubCompanyCountInfo()
	{
		String dataStr = CommonUtil.getCurDateStr();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT BU.name,to_char(s.last_upd,'YYYYMMDD'),count(*) FROM SIEBEL.S_ORG_EXT S ");
		sb.append(" LEFT JOIN SIEBEL.S_BU BU ON S.BU_ID = BU.ROW_ID ");
		sb.append(" WHERE BU.name IN ('一汽吉林','一汽解放','一汽轿车','天津一汽') ");
		sb.append(" AND to_char(s.last_upd,'YYYYMMDD')<'");
		sb.append(dataStr);
		sb.append("' group by BU.name,to_char(s.last_upd,'YYYYMMDD') ");
		sb.append(" ORDER BY to_char(s.last_upd,'YYYYMMDD') DESC");
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
				String subcomName = set.getString(1);
				String dateString = set.getString(2);
				int count = set.getInt(3);
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
		String dataObject1 = "/CRM/customer/MonSubComCountStatus";
		String keysCSV1 = "_keyfield";
		String xmlPayload1 = "<_MonSubComCountStatus><_keyfield>CRM</_keyfield></_MonSubComCountStatus>";
		DataObjectOperationByIDUtil.delete(dataObject1, keysCSV1, xmlPayload1);
		int size = dataList.size();
		if(size==0)
			return;
		if(size>19)
			size = 19;
		for(int i=0;i<size;i++)
		{
			Object[] objs = (Object[])dataList.get(i);
			String subcomName = (String)objs[0];
			String dateString = (String)objs[1];
			int count = ((Integer)objs[2]).intValue();
			String[] paras = { "subCompany#"+subcomName, 
					"day#"+dateString,
					"keyfield#CRM",
					"count#"+count};
			DataObjectOperationByIDUtil.insert("/CRM/customer/MonSubComCountStatus", paras);
		}
	}
	/**
	 * 取历史表中客户的数量
	 */
	private void getSDHCountInfo()
	{
		String dataStr = CommonUtil.getCurDateStr();
		StringBuffer sb = new StringBuffer();
		sb.append("select BU.name,a.ucm_type_cd,to_char(a.last_upd,'YYYYMMDD'),count(*)  from siebel.s_ucm_org_ext a ");
		sb.append(" left join  SIEBEL.S_BU BU ON a.BU_ID = BU.ROW_ID ");
		sb.append("  WHERE BU.name IN ('一汽吉林','一汽解放','一汽轿车','天津一汽')  ");
		sb.append(" AND to_char(a.last_upd,'YYYYMMDD')<'");
		sb.append(dataStr);
		sb.append("' group by BU.name,a.ucm_type_cd,to_char(a.last_upd,'YYYYMMDD') ");
		sb.append(" ORDER BY to_char(a.last_upd,'YYYYMMDD') DESC");
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
				Object[] objs = new Object[4];
				String subcomName = set.getString(1);
				String ucm_type_cd = set.getString(2);
				String dateString = set.getString(3);
				int count = set.getInt(4);
				objs[0] = subcomName;
				objs[1] =ucm_type_cd;
				objs[2] = dateString;
				objs[3] = new Integer(count);
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
		String dataObject1 = "/CRM/customer/MonSDHCountStatus";
		String keysCSV1 = "_keyfield";
		String xmlPayload1 = "<_MonSDHCountStatus><_keyfield>CRM</_keyfield></_MonSDHCountStatus>";
		DataObjectOperationByIDUtil.delete(dataObject1, keysCSV1, xmlPayload1);
		int size = dataList.size();
		if(size==0)
			return;
		if(size>19)
			size = 19;
		for(int i=0;i<size;i++)
		{
			Object[] objs = (Object[])dataList.get(i);
			String subcomName = (String)objs[0];
			String ucm_type_cd = (String)objs[1];
			String dateString =(String)objs[2];
			int count = ((Integer)objs[3]).intValue();
			String[] paras = { "subCompany#"+subcomName, 
					"ucmType#"+ucm_type_cd,
					"count#"+count,
					"keyfield#CRM",
					"day#"+dateString};
			DataObjectOperationByIDUtil.insert("/CRM/customer/MonSDHCountStatus", paras);
		}
	}
	/**
	 * 取客户分类的信息
	 */
	private void getAccountTypeInfo()
	{
		String dataStr = CommonUtil.getCurDateStr();
		StringBuffer sb = new StringBuffer();
		sb.append("select a.accnt_type_cd,count(*) from siebel.s_org_ext a ");
		sb.append("where to_char(a.last_upd,'YYYYMMDD')<'");
		sb.append(dataStr);
		sb.append("' group by a.accnt_type_cd order by count(*) desc");
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
				Object[] objs = new Object[2];
				String acctType = set.getString(1);
				int count = set.getInt(2);
				objs[0] = acctType;
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
		String dataObject1 = "/CRM/customer/MonAcctTypeStatus";
		String keysCSV1 = "_keyfield";
		String xmlPayload1 = "<_MonAcctTypeStatus><_keyfield>CRM</_keyfield></_MonAcctTypeStatus>";
		DataObjectOperationByIDUtil.delete(dataObject1, keysCSV1, xmlPayload1);
		if(dataList.size()==0)
			return;
		for(int i=0;i<dataList.size();i++)
		{
			Object[] objs = (Object[])dataList.get(i);
			String acctType = (String)objs[0];
			int count = ((Integer)objs[1]).intValue();
			String[] paras = { "acctType#"+acctType, 
					"keyfield#CRM",
					"count#"+count};
			DataObjectOperationByIDUtil.insert("/CRM/customer/MonAcctTypeStatus", paras);
		}
	}
	/**
	 * 取客户类别的信息
	 */
	private void getMarketTypeInfo()
	{
		String dataStr = CommonUtil.getCurDateStr();
		StringBuffer sb = new StringBuffer();
		sb.append("select a.market_type_cd,count(*) from siebel.s_org_ext a ");
		sb.append("where to_char(a.last_upd,'YYYYMMDD')<'");
		sb.append(dataStr);
		sb.append("' group by a.market_type_cd order by count(*) desc");
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
				Object[] objs = new Object[2];
				String marketType = set.getString(1);
				int count = set.getInt(2);
				objs[0] = marketType;
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
		String dataObject1 = "/CRM/customer/MonMarketTypeStatus";
		String keysCSV1 = "_keyfield";
		String xmlPayload1 = "<_MonMarketTypeStatus><_keyfield>CRM</_keyfield></_MonMarketTypeStatus>";
		DataObjectOperationByIDUtil.delete(dataObject1, keysCSV1, xmlPayload1);
		if(dataList.size()==0)
			return;
		for(int i=0;i<dataList.size();i++)
		{
			Object[] objs = (Object[])dataList.get(i);
			String marketType = (String)objs[0];
			int count = ((Integer)objs[1]).intValue();
			String[] paras = {"marketType#"+marketType, 
					"keyfield#CRM",
					"count#"+count};
			DataObjectOperationByIDUtil.insert("/CRM/customer/MonMarketTypeStatus", paras);
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
