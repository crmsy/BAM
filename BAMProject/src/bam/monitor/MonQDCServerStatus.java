package bam.monitor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import bam.jdbc.JDBCUtil;
import bam.monitor.control.MonCtrl;
import bam.util.CommonUtil;

public class MonQDCServerStatus implements MonCtrl {
	protected static Connection con;

	public void getDataInfo() {
		// TODO Auto-generated method stub
		getQDCServerStatus();
	}

	private void getQDCServerStatus()
	{
		String dateStr = " select distinct a.salecode salename,"+
		" substr(c.MAX_RECEIVE_TIME,0,19) MAX_SENDTIME,"+
		" 86400*(SYSDATE - TO_DATE(substr(c.MAX_RECEIVE_TIME,0,19),'YYYY-MM-DD HH24:MI:SS'))/60 INTERVALTIME "+
		  " from MONITOR_NODE_STATUS a "+
		 " inner join (select b.salecode, max(RECEIVE_TIME) MAX_RECEIVE_TIME,min(RECEIVE_TIME) MIN_RECEIVE_TIME "+
		  " from (select t.salecode, "+
		   " t.System_Name, "+
		    " t.Interface_Name, "+
		     " MAX(RECEIVE_TIME) RECEIVE_TIME "+
		      " from MONITOR_NODE_STATUS t "+
		       " group by t.salecode, t.System_Name, t.Interface_Name) b "+
		        " group by b.salecode)c "+
		    " on a.salecode = c.salecode "+
		   " and a.receive_time = c.MAX_RECEIVE_TIME ";
		PreparedStatement pm = null;
		ResultSet set = null;
		List dataList = new ArrayList();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(JDBCUtil.qdcurls, JDBCUtil.qdcusername, JDBCUtil.qdcpassward);
			pm = con.prepareStatement(dateStr);
			set = pm.executeQuery();
			
			while(set.next())
			{
				Object[] objs = new Object[4];
				String subcomName = set.getString("SALENAME");
				String dateString = set.getString("MAX_SENDTIME");
				int count = set.getInt("INTERVALTIME");
				objs[0] = subcomName;
				objs[1] = dateString;
				objs[2] = new Integer(count);
				if(count>CommonUtil.TIMEOUT){
					objs[3] = CommonUtil.SERVERSTATUS_STOP;
				}else{
					objs[3] = CommonUtil.SERVERSTATUS_RUNNING;
				}
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
		
		if(isInitial()){
			for(int i=0;i<dataList.size();i++)
			{
				Object[] objs = (Object[])dataList.get(i);
				String subcomName = (String)objs[0];
				String dateString = (String)objs[1];
				int count = ((Integer)objs[2]).intValue();
				String isConnected = (String)objs[3];
				StringBuffer sb = new StringBuffer();
				sb.append("insert into \"_MonQDCServerStatus\" (\"_BU_NAME\",\"_QDCSERVER_STATUS\",\"_MAX_SENDTIME\",\"_FRESHDATE\",\"SysIterID\",\"SysIterTrID\")");
				sb.append("values ('");
				sb.append(getSubComanyDesc(subcomName));
				sb.append("','");
				sb.append(isConnected);
				sb.append("',to_date('");
				sb.append(dateString);
				sb.append("','YYYY-MM-DD HH24:MI:SS'),sysdate,");
				sb.append("UTILSEQ.NEXTVAL");
				sb.append(",");
				sb.append("UTILSEQ.NEXTVAL");
				sb.append(")");
				JDBCUtil.setDataToBAM(sb.toString());
			}
		}
		else{
			for(int i=0;i<dataList.size();i++)
			{
				Object[] objs = (Object[])dataList.get(i);
				String subcomName = (String)objs[0];
				String dateString = (String)objs[1];
				int count = ((Integer)objs[2]).intValue();
				String isConnected = (String)objs[3];
				StringBuffer sb = new StringBuffer();
				sb.append("UPDATE \"_MonQDCServerStatus\" SET \"_QDCSERVER_STATUS\" = '");
				sb.append(isConnected);
				sb.append("',");
				sb.append("\"_FRESHDATE\" = SYSDATE WHERE \"_BU_NAME\" = '");
				sb.append(getSubComanyDesc(subcomName));
				sb.append("'");
				JDBCUtil.setDataToBAM(sb.toString());
			}
		}
	}
	
	

	private String getSubComanyDesc(Object object) {
		// TODO Auto-generated method stub
		String companyName = "";
		String str = (String) object;
		if(str.equals("HQDOWN")){
			companyName = "一汽轿车";
		}
		if(str.equals("JFDOWN")){
			companyName = "一汽解放";
		}
		if(str.equals("JLDOWN")){
			companyName = "一汽吉林";
		}
		if(str.equals("TJDOWN")){
			companyName = "天津一汽";
		}
		if(str.equals("000000")){
			companyName = "一汽集团";
		}
		return companyName;
	}

	private boolean isInitial() {
		// TODO Auto-generated method stub
		boolean isInitial = false;
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT count(*) from \"_MonQDCServerStatus\"");
		PreparedStatement pm = null;
		ResultSet set = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(JDBCUtil.urls, JDBCUtil.username1, JDBCUtil.passward1);
			pm = con.prepareStatement(sb.toString());
			set = pm.executeQuery();
			while(set.next())
			{
				int count = set.getInt(1);
				
				if(count==0){
					isInitial = true;
				}
				else{
					isInitial = false;
				}
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
		return isInitial;
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
