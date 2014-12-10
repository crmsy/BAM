package bam.util;

import java.util.ArrayList;
import java.util.Calendar;

public class CommonUtil {
	/**
	 * ȡ��ǰ���ڵ��ַ���
	 * @return
	 */
	public static String getCurDateStr()
	{
		String str = "";
		Calendar cal = Calendar.getInstance();    
		int year = cal.get(Calendar.YEAR);   
		int month = cal.get(Calendar.MONTH)+1;   
		int day = cal.get(Calendar.DATE);    
		str = ""+year+(month<=9?"0"+month:month)+day;
		return str;
	}
	public static String SERVERSTATUS_RUNNING = "RUNNING";
	
	public static String SERVERSTATUS_STOP = "STOP";

	public static ArrayList<String> SUBCOMPANYLIST;
	
	public static ArrayList<String> getSubCompany()
	{
		if(SUBCOMPANYLIST == null){
			SUBCOMPANYLIST = new ArrayList<String>();
			SUBCOMPANYLIST.add("һ���γ�");
			SUBCOMPANYLIST.add("һ�����");
			SUBCOMPANYLIST.add("һ������");
			SUBCOMPANYLIST.add("���һ��");
		}
		return SUBCOMPANYLIST;
	}
	
	public static int TIMEOUT = 60;
}
