package edu.ucsb.mnl.mona.test;

import edu.ucsb.mnl.mona.CassandraDirect;
import edu.ucsb.mnl.mona.gcam.GCAMBeanList;

public class BeanTest {

	public static void main(String[] args) {
		GCAMBeanList gcbl = CassandraDirect.readGCAMBeans("gcam_mappings.json");
		for ( int i = 0; i < gcbl.data.size(); i++ ) {
			System.out.println( gcbl.data.get(i).Variable);
		}
	}

}
