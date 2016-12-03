/**
 * 
 */
package edu.nyu.adbms;

import java.io.BufferedReader;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Pratyush Anand (pa1139)
 * @author Arpit Saini (ads745)
 *
 */
public class TransactionManager {
	private Date timestamp;
	private BufferedReader br;
	private Map<Integer, Transaction> transactions = new HashMap<Integer, Transaction>();
	private List<DatabaseManager> dm; 
}
