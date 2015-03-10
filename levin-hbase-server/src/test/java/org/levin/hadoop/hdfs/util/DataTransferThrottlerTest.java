package org.levin.hadoop.hdfs.util;

import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Test;

public class DataTransferThrottlerTest {
	@Test
	public void testThrottle() throws Exception {
		DataTransferThrottler throttler = new DataTransferThrottler(100, 1000);
		
		Thread.sleep(10);
		throttler.throttle(100000);
	}
}
