package bam.monitor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import bam.jdbc.JDBCUtil;
import bam.monitor.control.MonCtrl;
import bam.util.CommonUtil;

public class MonDBServerStatus implements MonCtrl {
	protected static Connection con;

	public void getDataInfo() {
		// TODO Auto-generated method stub
		getDBServerStatus();
	}

	private void getDBServerStatus()
	{
		String isConnected = "RUNNING";
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(JDBCUtil.urls, JDBCUtil.username, JDBCUtil.passward);
			if(con.isClosed()){
				isConnected = CommonUtil.SERVERSTATUS_STOP;
			}
			else {
				isConnected = CommonUtil.SERVERSTATUS_RUNNING;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeConnection();
		}
		
		if(isInitial()){
			for(int i=0;i<CommonUtil.getSubCompany().size();i++)
			{
				StringBuffer sb = new StringBuffer();
				sb.append("insert into \"_MonDBServerStatus\" (\"_BU_NAME\",\"_DBSERVER_STATUS\",\"_FRESHDATE\",\"SysIterID\",\"SysIterTrID\")");
				sb.append("values ('");
				sb.append(CommonUtil.getSubCompany().get(i));
				sb.append("','");
				sb.append(isConnected);
				sb.append("',sysdate,");
				sb.append("UTILSEQ.NEXTVAL");
				sb.append(",");
				sb.append("UTILSEQ.NEXTVAL");
				sb.append(")");
				JDBCUtil.setDataToBAM(sb.toString());
			}
		}
		else{
			for(int i=0;i<CommonUtil.getSubCompany().size();i++)
			{
				StringBuffer sb = new StringBuffer();
				sb.append("UPDATE \"_MonDBServerStatus\" SET \"_DBSERVER_STATUS\" = '");
				sb.append(isConnected);
				sb.append("',");
				sb.append("\"_FRESHDATE\" = SYSDATE WHERE \"_BU_NAME\" = '");
				sb.append(CommonUtil.getSubCompany().get(i));
				sb.append("'");
				JDBCUtil.setDataToBAM(sb.toString());
			}
		}
	}
	
	private boolean isInitial() {
		// TODO Auto-generated method stub
		boolean isInitial = false;
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT count(*) from \"_MonDBServerStatus\"");
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
