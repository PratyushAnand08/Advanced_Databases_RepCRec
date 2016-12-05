/**
 * 
 */
package edu.nyu.adbms;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;

import edu.nyu.adbms.Operation.Type;

import java.util.Map.Entry;

/**
 * @author Pratyush Anand (pa1139)
 * @author Arpit Saini (ads745)
 *
 */
public class TransactionManager {
	
	private int _timestamp;
	private Scanner _sc;
	
	private List<DatabaseManager> _databaseManagers;
	// map of transid and transaction
	private Map<Integer, Transaction> _transactions = new HashMap<Integer, Transaction>();
    //map of site index and the list of variables stored in it
	private Map<Integer, Queue<Operation>> _transOperationMap = new HashMap<Integer, Queue<Operation>>();
	
	private Map<Operation, List<Integer>> _operationLockAcquiredSitesMap = new HashMap<Operation, List<Integer>>();
	private Map<Integer, List<Integer>> _variableMap;
	//Set of aborted transactions
	private Set<Integer> _abortedTransactions = new HashSet<Integer>();
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
		_timestamp = 0;
		_databaseManagers = new ArrayList<DatabaseManager>();
		for (int i=1; i <= n; i++) {
			DatabaseManager dm = new DatabaseManager(i, this);
			_databaseManagers.add(dm);
		}
		
		_variableMap = new HashMap<Integer, List<Integer>>();
		for (int i = 1; i <= 20; i++) {
			List<Integer> siteList = new ArrayList<Integer>();
		    if (i % 2 == 1) {
		    	siteList.add(1 + i % 10);
		    } else {
		    	for (int j = 1; j <= 10; j++) {
		    		siteList.add(j);
		        }
		    }
		    _variableMap.put(i, siteList);
		}
		
		/*Iterator<Entry<Integer, List<Integer>>> it = _variableMap.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<Integer, List<Integer>> pair = (Map.Entry<Integer, List<Integer>>)it.next();
			for(DatabaseManager dm : _databaseManagers){
				
				if(dm.getSiteIndex() == pair.getKey()) {
					for (Integer variable: pair.getValue()) {
						Data data = new Data(10 * variable, variable);
						variableList.add(data);
					}
					dm.set_variableList(variableList);
				}
			}
		}*/
	}
	
	/**
	 * @return the _timestamp
	 */
	public int get_timestamp() {
		return _timestamp;
	}

	public void start() {
		try {
			while(_sc.hasNextLine()) {
				String readLine = _sc.nextLine();
				if (readLine == null || readLine.contains("exit"))
					break;
				if (readLine.startsWith("//"))
					continue;
				if (!readLine.isEmpty()) {
					parseLine(readLine);
				}
				_timestamp++;
			}
			_sc.close();
		}
		catch (Exception e) {
			System.err.println("TM -> start(): "+e.getMessage());
			e.printStackTrace();
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
				case "begin" : beginTransaction("RW", Integer.parseInt(args.substring(1)));
						    break;
				case "beginRO" : beginTransaction("RO", Integer.parseInt(args.substring(1)));
							break;
				case "R" : parseReadWrite(args);
							break;
				case "W" : parseReadWrite(args);
							break;
				case "end" : endTransaction(args);//to be done
							break;
				case "fail" : failSite(Integer.parseInt(args));
							break;
				case "recover" : recoverSite(Integer.parseInt(args));
							break;
				case "dump" : dump(args);//done
							break;
				case "querystate" : queryState();//to be done
							break;
				default : System.out.println("Exiting from Database");	
						  System.exit(1);
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
				System.out.println("---------- Site "+dm.getSiteIndex()+" ----------");
				dm.dumpSite();
				System.out.println();
			}
		}
	}

	private void dumpVariable(int variable) {
		// TODO Auto-generated method stub
		List<Integer> siteList = _variableMap.get(variable);
		for (Integer site : siteList) {
			DatabaseManager dm = _databaseManagers.get(site-1);
			System.out.println("---------- Site "+dm.getSiteIndex()+" ----------");
			dm.dumpVariable(variable);
			System.out.println();
		}
	}

	private void dumpSite(int site) {
		// TODO Auto-generated method stub
		for(DatabaseManager dm : _databaseManagers){
			if(dm.getSiteIndex() == site){
				System.out.println("---------- Site "+dm.getSiteIndex()+" ----------");
				dm.dumpSite();
				System.out.println();
			}
		}
	}

	private void recoverSite(int site) {
		// TODO Auto-generated method stub
		_databaseManagers.get(site-1).recover();
		System.out.println("Site "+site+" recovered.");
	}

	private void failSite(int site) {
		// TODO Auto-generated method stub
		List<Operation> operation = _databaseManagers.get(site-1).getAllowedOperations();
		for (Operation op : operation) {
			abort(op.get_transactionId());
			for(Iterator<Entry<Integer, Queue<Operation>>> it = _transOperationMap.entrySet().iterator(); it.hasNext(); ) {
			    Entry<Integer, Queue<Operation>> entry = it.next();
			    if(entry.getKey().equals(op.get_transactionId())) {
			        it.remove();
			    }
			}
			System.out.println("T"+op.get_transactionId()+" aborted.");
		}
		_databaseManagers.get(site-1).fail();
		System.out.println("Above transactions were aborted as site "+site+" failed");
	}

	private void abort(Integer trans) {
		// TODO Auto-generated method stub
		for(DatabaseManager dm : _databaseManagers){
			dm.abortTransaction(trans);
		}
		_abortedTransactions.add(trans);
	}

	private void endTransaction(String args) {
		// TODO Auto-generated method stub
		Integer transaction = Integer.parseInt(args.substring(1));
		if(_abortedTransactions.contains(transaction))
			return;
		Queue<Operation> opQueue = _transOperationMap.get(transaction);
		for(Operation op : opQueue) {
			List<Integer> siteList = _operationLockAcquiredSitesMap.get(op);
			for(Integer site : siteList) {
				DatabaseManager dm = _databaseManagers.get(site-1);
				dm.commit(op);
			}
			_operationLockAcquiredSitesMap.remove(op);
		}
		_transOperationMap.remove(transaction);
		_transactions.remove(transaction);
		System.out.println("Transaction T"+transaction+" committed successfully.");
		checkToReleaseWaitingTransactions();
	}

	private void checkToReleaseWaitingTransactions() {
		// TODO Auto-generated method stub
		Operation tempOperation;
		while(_waitingOperations.peek() != null) {
			tempOperation = _waitingOperations.poll();
			if(tempOperation.get_type() == Type.READ){
				read(tempOperation);
			}
			else {
				write(tempOperation);
			}
		}
	}

	private void parseReadWrite(String args) {
		// TODO Auto-generated method stub
		System.out.println("Inside parseReadWrite()");
		String[] transVarData = args.split(",");
		if(transVarData.length == 2) {
			Operation op = new Operation(Integer.parseInt(transVarData[0].substring(1)), Operation.Type.READ,
					Integer.parseInt(transVarData[1].substring(1)), new Date());
			read(op);
		}
		else if(transVarData.length == 3) {
			Operation op = new Operation(Integer.parseInt(transVarData[0].substring(1)), Operation.Type.WRITE, 
					Integer.parseInt(transVarData[1].substring(1)), 
					Integer.parseInt(transVarData[2]), new Date());
			write(op);
		}
		else {
			System.out.println("Wrong number of arguments provided for read or write command. Arguments cannot be "+ args);
			return;
		}
	}

	private void write(Operation op) {
		// TODO Auto-generated method stub
		System.out.println("TM() -> Inside write()");
		boolean result = false;
		List<Integer> siteList = new ArrayList<Integer>();
		if(_abortedTransactions.contains(op.get_transactionId()))
			return;
		List<Integer> sites = _variableMap.get(op.get_varIndex());
		for(Integer site : sites) {
			DatabaseManager dm = _databaseManagers.get(site-1);
			if(dm.getSiteStatus()) {
				result = dm.acquireLock(op);
				if(result == true) {
					siteList.add(dm.getSiteIndex());
				}
			}
			_operationLockAcquiredSitesMap.put(op, siteList);
		}
		if(siteList.size() == 0) {
			_waitingOperations.add(op);
			System.out.println("TM, write() -> Didnt' get lock. In waiting.");
		}
		else {
			System.out.println("TM, write() -> Got the lock.");
			Queue<Operation> operationQueue = _transOperationMap.get(op.get_transactionId());
			if(operationQueue != null) {
				operationQueue.add(op);
				_transOperationMap.put(op.get_transactionId(), operationQueue);
			}
			else {
				Queue<Operation> newOperationQueue = new LinkedList<Operation>();
				newOperationQueue.add(op);
				_transOperationMap.put(op.get_transactionId(), newOperationQueue);
			}
		}
	}

	private void read(Operation op) {
		// TODO Auto-generated method stub
		System.out.println("TM() -> Inside read()");
		boolean result = false;
		Transaction transaction;
		if(_abortedTransactions.contains(op.get_transactionId()))
			return;
		if(_transactions.get(op.get_transactionId()).get_type() == Transaction.Type.RO) {
			//take snapshot
			result = true;
		}
		else {
			List<Integer> sites = _variableMap.get(op.get_varIndex());
			for (Integer site : sites) {
				DatabaseManager dm = _databaseManagers.get(site-1);
				if(dm.getSiteStatus() && dm.isVarAccessible(op.get_varIndex())) {
					result = dm.acquireLock(op);
					if(result == true){
						List<Integer> siteList = new ArrayList<Integer>();
						siteList.add(dm.getSiteIndex());
						_operationLockAcquiredSitesMap.put(op, siteList);
						break;
					}
				}
			}
		}
		if(!result) {
			_waitingOperations.add(op);
			System.out.println("TM, read() -> Didnt' get lock. In waiting.");
		}
		else {
			System.out.println("TM, read() -> Got the lock.");
			Queue<Operation> operationQueue = _transOperationMap.get(op.get_transactionId());
			if(operationQueue != null) {
				operationQueue.add(op);
				_transOperationMap.put(op.get_transactionId(), operationQueue);
			}
			else {
				Queue<Operation> newOperationQueue = new LinkedList<Operation>();
				newOperationQueue.add(op);
				_transOperationMap.put(op.get_transactionId(), newOperationQueue);
			}
		}
	}

	private List<Integer> getSiteForVariable(int var) {
		// TODO Auto-generated method stub
		return _variableMap.get(var);
	}

	private void beginTransaction(String type, int transId) {
		// TODO Auto-generated method stub
		if(type == "RW"){
			_transactions.put(transId, new Transaction(transId, _timestamp, Transaction.Type.RW));
		}
		else {
			_transactions.put(transId, new Transaction(transId, _timestamp, Transaction.Type.RO));
		}
	}

	public void CreateWaitingGraph(int get_transactionId, int transId) {
		// TODO Auto-generated method stub
		
	}
	
}
