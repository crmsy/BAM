package bam.timer;

import java.util.Timer;

import bam.monitor.MonCustomerStatus;

public class BamTimer {

	public static void main(String[] args)
	{
		Timer t = new Timer();
		t.schedule(new runMonitor(), 1000, 5000);
	}
	
	static class runMonitor extends java.util.TimerTask
	{
		@Override
		public void run() {
			// TODO Auto-generated method stub
			MonCustomerStatus customer =new MonCustomerStatus();
			customer.getDataInfo();
		}
	}
}



