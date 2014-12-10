package bam.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class JDBCUtil {
	public static String urls = "jdbc:oracle:thin:@10.133.83.33:1521:SITUCM";
	protected static Connection con;
	public static String username = "FAWADMIN";
	public static String passward = "FAWADMIN";

	public static String username1 = "DEV_ORABAM";
	// protected static String username = "DEV_SOAINFRA";
	public static String passward1 = "welcome1";

	public static void openGetConnection() {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			// Class.forName("oracle.jdbc.xa.client.OracleXADataSource");
			con = DriverManager.getConnection(urls, username, passward);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void openSetConnection() {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			// Class.forName("oracle.jdbc.xa.client.OracleXADataSource");
			con = DriverManager.getConnection(urls, username1, passward1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void closeConnection() {
		try {
			if (con != null && !con.isClosed()) {
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	public static ResultSet getDataFromBAM(String sql) {
		ResultSet set = null;
		PreparedStatement pm = null;
		try {
			openSetConnection();
			pm = con.prepareStatement(sql);
			set = pm.executeQuery();

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
		return set;
	}
	public static void setDataToBAM(String sql) {
		PreparedStatement pm = null;
		try {
			openSetConnection();
			pm = con.prepareStatement(sql);
			pm.execute(sql);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {

				if (pm != null) {
					pm.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			closeConnection();
		}
	}
}
