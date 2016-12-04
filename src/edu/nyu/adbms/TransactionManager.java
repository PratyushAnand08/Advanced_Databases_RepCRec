/**
 * 
 */
package edu.nyu.adbms;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;

/**
 * @author Pratyush Anand (pa1139)
 * @author Arpit Saini (ads745)
 *
 */
public class TransactionManager {
	
	private Date _timestamp;
	private Scanner _sc;
	
	private List<DatabaseManager> _databaseManagers;
	// map of transid and transaction
	private Map<Integer, Transaction> _transactions = new HashMap<Integer, Transaction>();
    //map of site index and the list of variables stored in it
	private Map<Integer, List<Integer>> _variableMap;
	//Set of aborted transactions
	private Set<Integer> _abortedTransactions = new HashSet<Integer>();
	//Set of committed trans
	private Set<Integer> _committedTransactions = new HashSet<Integer>();
	//Queue of waiting operations
	private Queue<Operation> _waitingOperations = new LinkedList<Operation>();
	
	public TransactionManager() {
		_sc = new Scanner(System.in);
	}
	
	public TransactionManager(String inputFileName) {
		try {
			_sc = new Scanner(inputFileName);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	public void init(int n) {
		_timestamp = new Date();
		_databaseManagers = new ArrayList<DatabaseManager>();
		for (int i=1; i <= n; i++) {
			DatabaseManager dm = new DatabaseManager(i);
			_databaseManagers.add(dm);
		}
		_variableMap = new HashMap<Integer, List<Integer>>();
		for (int i=1; i <= 20; i++) {
			List<Integer> siteList = new ArrayList<Integer>();
			if (i % 2 == 0){
				siteList.add(i);
			}
			else {
				siteList.add(1 + i % 10);
			}
			_variableMap.put(i, siteList);
		}
	}
	
	public void start() {
		try {
			while(sc.hasNextLine()) {
				String readLine = sc.nextLine();
				if (readLine == null || readLine.contains("exit"))
					break;
				if (readLine.startsWith("//"))
					continue;
				if (!readLine.isEmpty()) {
					parseLine(readLine);
				}
			}
			sc.close();
		}
		catch (Exception e) {
			System.err.println("TM -> start(): "+e.getMessage());
		}
	}

	private void parseLine(String line) {
		// TODO Auto-generated method stub
		int openingBracket = -1, closingBracket = -1;
		String[] commands = line.replaceAll("\\s+", "").split(";");
		for (String command : commands) {
			openingBracket = command.indexOf("(");
			closingBracket = command.indexOf(")");
			if ( openingBracket == -1 || closingBracket == -1) {
				System.out.println("Wrong command: "+ command);
				break;
			}
			String commandType = command.substring(0, openingBracket);
			String args = command.substring(openingBracket + 1, closingBracket);
			switch(commandType) {
				case "begin" : beginTransaction("RW", Integer.parseInt(args.substring(1)));//done
						    break;
				case "beginRO" : beginTransaction("RO", Integer.parseInt(args.substring(1)));//done
							break;
				case "R" : parseReadWrite(args);//to be done
							break;
				case "W" : parseReadWrite(args);//to be done
							break;
				case "end" : endTransaction(args);//to be done
							break;
				case "fail" : failSite(Integer.parseInt(args));//done
							break;
				case "recover" : recoverSite(Integer.parseInt(args));//done
							break;
				case "dump" : dump(args);//done
							break;
				case "querystate" : queryState();//to be done
							break;	 
			}
		}
	}

	private void queryState() {
		// TODO Auto-generated method stub
		
	}

	private void dump(String args) {
		// TODO Auto-generated method stub
		if(!args.isEmpty()) {
			if(args.startsWith("x")){
				dumpVariable(Integer.parseInt(args.substring(1)));
			}
			else{
				dumpSite(Integer.parseInt(args));
			}
		}
		else {
			for(DatabaseManager dm : _databaseManagers){
				dm.dumpSite();
			}
		}
	}

	private void dumpVariable(int variable) {
		// TODO Auto-generated method stub
		for(DatabaseManager dm : _databaseManagers){
			dm.dumpVariable(variable);
		}
	}

	private void dumpSite(int site) {
		// TODO Auto-generated method stub
		for(DatabaseManager dm : _databaseManagers){
			if(dm.getSiteIndex() == site){
				dm.dumpSite();
			}
		}
	}

	private void recoverSite(int site) {
		// TODO Auto-generated method stub
		_databaseManagers.get(site).recover();
		System.out.println("Site "+site+" recovered.");
	}

	private void failSite(int site) {
		// TODO Auto-generated method stub
		List<Integer> transaction = _databaseManagers.get(site).getAllAccessedTransactions();
		for (Integer trans : transaction) {
			_abortedTransactions.add(trans);
			abort(trans);
			System.out.println("T"+trans+" aborted.");
		}
		_databaseManagers.get(site).fail();
		System.out.println("All the above transactions were aborted as site "+site+" failed");
	}

	private void abort(Integer trans) {
		// TODO Auto-generated method stub
		
	}

	private void endTransaction(String args) {
		// TODO Auto-generated method stub
		
	}

	private void parseReadWrite(String args) {
		// TODO Auto-generated method stub
		String[] transVarData = args.split(",");
		if(transVarData.length == 2) {
			read(transVarData[0], transVarData[1]);
		}
		else if(transVarData.length == 3) {
			write(transVarData[0], transVarData[1], transVarData[2]);
		}
		else {
			System.out.println("Wrong number of arguments provided for read or write command. Arguments cannot be "+ args);
			return;
		}
	}

	private void write(String transId, String variable, String value) {
		// TODO Auto-generated method stub
		
	}

	private void read(String transId, String variable) {
		// TODO Auto-generated method stub
		int id = Integer.parseInt(transId.substring(1));
		int var = Integer.parseInt(variable.substring(1));
		if(_abortedTransactions.contains(id))
			return;
		Operation op = new Operation(id, Operation.Type.READ, var, new Date());
		List<Integer> sites = getSiteForVariable(var);
		for(Integer site : sites) {
			DatabaseManager dm = _databaseManagers.get(site-1);
			if(dm.getSiteStatus()) {
				
			}
		}
	}

	private List<Integer> getSiteForVariable(int var) {
		// TODO Auto-generated method stub
		return null;
	}

	private void beginTransaction(String type, int transId) {
		// TODO Auto-generated method stub
		if(type == "RW"){
			_transactions.put(transId, new Transaction(transId, new Date(), Transaction.Type.RW));
		}
		else {
			_transactions.put(transId, new Transaction(transId, new Date(), Transaction.Type.RO));
		}
	}
	
	
}
