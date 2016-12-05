/**
 * 
 */
package edu.nyu.adbms;

/**
 * @author pratyush1
 *
 */
public class DistributedDatabase {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TransactionManager tm;
		
		if(args.length == 0){
			tm = new TransactionManager();
		}
		else {
			tm = new TransactionManager(args[0]);
		}
		
		tm.init(10);
		tm.start();
	}

}
