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

public class MonRequestStatus implements MonCtrl {
	protected static Connection con;

	@Override
	public void getDataInfo() {
		// TODO Auto-generated method stub
		getSubCompantStatusInfo();
		getSRTypeStatusInfo();
		getSRDealerStatusInfo();
		getSRStatusInfo();
	}

	private void getSubCompantStatusInfo() {
		String dataStr = CommonUtil.getCurDateStr();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT BU.name,to_char(S.last_upd,'YYYYMMDD'),count(*) FROM SIEBEL.S_SRV_REQ S ");
		sb.append(" LEFT JOIN SIEBEL.S_BU BU ON S.BU_ID = BU.ROW_ID ");
		sb.append(" WHERE BU.name IN ('一汽吉林','一汽解放','一汽轿车','天津一汽') ");
		sb.append(" AND to_char(S.last_upd,'YYYYMMDD')<'");
		sb.append(dataStr);
		sb.append("' group by BU.name,to_char(S.last_upd,'YYYYMMDD') ");
		sb.append(" ORDER BY to_char(S.last_upd,'YYYYMMDD') DESC");
		PreparedStatement pm = null;
		ResultSet set = null;
		List dataList = new ArrayList();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(JDBCUtil.urls, JDBCUtil.username,
					JDBCUtil.passward);
			pm = con.prepareStatement(sb.toString());
			set = pm.executeQuery();

			while (set.next()) {
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
		String dataObject1 = "/CRM/sr/MonSRSubComCountStatus";
		String keysCSV1 = "_keyfield";
		String xmlPayload1 = "<_MonSRSubComCountStatus><_keyfield>CRM</_keyfield></_MonSRSubComCountStatus>";
		DataObjectOperationByIDUtil.delete(dataObject1, keysCSV1, xmlPayload1);
		int size = dataList.size();
		if (size == 0)
			return;
		if (size > 19)
			size = 19;
		for (int i = 0; i < size; i++) {
			Object[] objs = (Object[]) dataList.get(i);
			String subcomName = (String) objs[0];
			String dateString = (String) objs[1];
			int count = ((Integer) objs[2]).intValue();
			String[] paras = { "subCompany#"+subcomName, 
					"day#"+dateString,
					"count#"+count,
					"keyfield#CRM"};
			DataObjectOperationByIDUtil.insert("/CRM/sr/MonSRSubComCountStatus", paras);
		}
	}

	/**
	 * 取工单类型的信息
	 */
	private void getSRTypeStatusInfo() {
		String dataStr = CommonUtil.getCurDateStr();
		StringBuffer sb = new StringBuffer();
		sb
				.append(" SELECT bu.name,s.sr_type_cd,to_char(S.last_upd,'YYYYMMDD'),count(*) FROM SIEBEL.S_SRV_REQ S ");
		sb.append(" LEFT JOIN SIEBEL.S_BU BU ON S.BU_ID = BU.ROW_ID ");
		sb.append(" WHERE BU.name IN ('一汽吉林','一汽解放','一汽轿车','天津一汽') ");
		sb.append(" AND to_char(S.last_upd,'YYYYMMDD')<'");
		sb.append(dataStr);
		sb
				.append("' group by bu.name,s.sr_type_cd,to_char(S.last_upd,'YYYYMMDD') ");
		sb.append(" ORDER BY to_char(S.last_upd,'YYYYMMDD') DESC");
		PreparedStatement pm = null;
		ResultSet set = null;
		List dataList = new ArrayList();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(JDBCUtil.urls, JDBCUtil.username,
					JDBCUtil.passward);
			pm = con.prepareStatement(sb.toString());
			set = pm.executeQuery();

			while (set.next()) {
				Object[] objs = new Object[4];
				String subcomName = set.getString(1);
				String srType = set.getString(2);
				String dateString = set.getString(3);
				int count = set.getInt(4);
				objs[0] = subcomName;
				objs[1] = srType;
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
		String dataObject1 = "/CRM/sr/MonSRTypeStatus";
		String keysCSV1 = "_keyfield";
		String xmlPayload1 = "<_MonSRTypeStatus><_keyfield>CRM</_keyfield></_MonSRTypeStatus>";
		DataObjectOperationByIDUtil.delete(dataObject1, keysCSV1, xmlPayload1);
		int size = dataList.size();
		if (size == 0)
			return;
		if (size > 19)
			size = 19;
		for (int i = 0; i < size; i++) {
			Object[] objs = (Object[]) dataList.get(i);
			String subcom = (String) objs[0];
			String srType = (String)objs[1];
			String dateString = (String)objs[2];
			int count = ((Integer) objs[3]).intValue();
			String[] paras = { "subCompany#"+subcom, 
					"srType#"+srType,
					"day#"+dateString,
					"count#"+count,
					"keyfield#CRM"};
			DataObjectOperationByIDUtil.insert("/CRM/sr/MonSRTypeStatus", paras);
		}
	}

	/**
	 * 取受理人的状态信息
	 */
	private void getSRDealerStatusInfo() {
		String dataStr = CommonUtil.getCurDateStr();
		StringBuffer sb = new StringBuffer();
		sb.append("select s.ALT_CON_EMAIL,count(*) from siebel.s_srv_req s ");
		sb.append(" where to_char(S.last_upd,'YYYYMMDD') < '");
		sb.append(dataStr);
		sb.append("' group by s.ALT_CON_EMAIL");
		PreparedStatement pm = null;
		ResultSet set = null;
		List dataList = new ArrayList();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(JDBCUtil.urls, JDBCUtil.username,
					JDBCUtil.passward);
			pm = con.prepareStatement(sb.toString());
			set = pm.executeQuery();

			while (set.next()) {
				Object[] objs = new Object[3];
				String dealerName = set.getString(1);
				int count = set.getInt(2);
				objs[0] = dealerName;
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
		String dataObject1 = "/CRM/sr/MonSRDealerStatus";
		String keysCSV1 = "_keyfield";
		String xmlPayload1 = "<_MonSRDealerStatus><_keyfield>CRM</_keyfield></_MonSRDealerStatus>";
		DataObjectOperationByIDUtil.delete(dataObject1, keysCSV1, xmlPayload1);
		int size = dataList.size();
		if (size == 0)
			return;
		if (size > 19)
			size = 19;
		List temp = new ArrayList();
		for (int i = 0; i < size; i++) {
			Object[] objs = (Object[]) dataList.get(i);
			String name = (String) objs[0];
			if(name==null||temp.contains(name))
				continue;
			temp.add(name);
			int count = ((Integer) objs[1]).intValue();
			String[] paras = { "name#"+name, 
					"count#"+count,
					"keyfield#CRM"};
			DataObjectOperationByIDUtil.insert("/CRM/sr/MonSRDealerStatus", paras);
		}
	}

	/**
	 * 取工单的状态
	 */
	private void getSRStatusInfo() {
		String dataStr = CommonUtil.getCurDateStr();
		StringBuffer sb = new StringBuffer();
		sb.append("select s.SR_STAT_ID,count(*) from siebel.s_srv_req s ");
		sb.append(" where to_char(S.last_upd,'YYYYMMDD') < '");
		sb.append(dataStr);
		sb.append("' group by s.SR_STAT_ID");
		PreparedStatement pm = null;
		ResultSet set = null;
		List dataList = new ArrayList();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(JDBCUtil.urls, JDBCUtil.username,
					JDBCUtil.passward);
			pm = con.prepareStatement(sb.toString());
			set = pm.executeQuery();

			while (set.next()) {
				Object[] objs = new Object[3];
				String status = set.getString(1);
				int count = set.getInt(2);
				objs[0] = status;
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
		String dataObject1 = "/CRM/sr/MonSRCompleteStatus";
		String keysCSV1 = "_keyfield";
		String xmlPayload1 = "<_MonSRCompleteStatus><_keyfield>CRM</_keyfield></_MonSRCompleteStatus>";
		DataObjectOperationByIDUtil.delete(dataObject1, keysCSV1, xmlPayload1);
		int size = dataList.size();
		if (size == 0)
			return;
		if (size > 19)
			size = 19;
		List temp = new ArrayList();
		for (int i = 0; i < size; i++) {
			Object[] objs = (Object[]) dataList.get(i);
			String status = (String) objs[0];
			if(status==null||temp.contains(status))
				continue;
			temp.add(status);
			int count = ((Integer) objs[1]).intValue();
			String[] paras = { "status#"+status, 
					"count#"+count,
					"keyfield#CRM"};
			DataObjectOperationByIDUtil.insert("/CRM/sr/MonSRCompleteStatus", paras);
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

	private long getSeqNO() {
		long maxId = 1;
		PreparedStatement pm = null;
		ResultSet set = null;
		StringBuffer sb = new StringBuffer();
		sb.append(" select max(\"SysIterID\") from \"_MonSDHCountStatus\" t ");
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(JDBCUtil.urls,
					JDBCUtil.username1, JDBCUtil.passward1);
			pm = con.prepareStatement(sb.toString());
			set = pm.executeQuery();
			while (set.next()) {
				maxId = set.getLong(1);
				break;
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
		return maxId;
	}
}
