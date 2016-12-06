/**
 * 
 */
package edu.nyu.adbms;

/**
 * @author Pratyush Anand (pa1139)
 * @author Arpit Saini (ads745)
 *
 */
public class DistributedDatabase {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TransactionManager tm;
		System.out.println(">>>>> Enter Database <<<<<");
		System.out.println();
		System.out.println("Please enter your commands -->");
		
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
