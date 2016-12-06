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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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
	private BufferedReader br;
	
	//List of all DatabaseManagers.
	private List<DatabaseManager> _databaseManagers;
	
	//map of transId, transaction(transid)
	private Map<Integer, Transaction> _transactions = new HashMap<Integer, Transaction>();
	
    //map of varId, List of sites containing that varId
	private Map<Integer, List<Integer>> _variableMap;
	
	//map of transId, Queue or operations associated with that transId
	private Map<Integer, Queue<Operation>> _transOperationMap = new HashMap<Integer, Queue<Operation>>();
	
	//map of transId_, List of transIds
	private Map<Integer, List<Integer>> _transLockTransMap = new HashMap<Integer, List<Integer>>();
	
	//
	private Map<Operation, List<Integer>> _operationLockAcquiredSitesMap = new HashMap<Operation, List<Integer>>();
	
	private Map<Integer, List<List<Data>>> _transSiteDataList = new HashMap<Integer, List<List<Data>>>();
	
	//Set of aborted transactions
	private Set<Integer> _abortedTransactions = new HashSet<Integer>();
	//Queue of waiting operations
	private Queue<Operation> _waitingOperations = new LinkedList<Operation>();
	
	/**
	 * 
	 * Contructor when no argument(filename) is passed
	 * @param none
	 * @return none
	 * 
	 */
	public TransactionManager() {
		br = new BufferedReader(new InputStreamReader(System.in));
	}
	
	/**
	 * 
	 * Contructor when argument(filename) is passed
	 * @param inputFileName
	 * @return none
	 * 
	 */
	public TransactionManager(String inputFileName) {
		try {
			br = new BufferedReader(new FileReader(inputFileName));
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	/**
	 * 
	 * Initializes all the sites and variables associated with a particular site
	 * @param number of sites
	 * @return none
	 * 
	 */
	public void init(int n) {
		_timestamp = 0;
		_databaseManagers = new ArrayList<DatabaseManager>();
		for (int i=1; i <= n; i++) {
			DatabaseManager dm = new DatabaseManager(i);
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
	}
	
	/**
	 * @return the _timestamp
	 */
	public int get_timestamp() {
		return _timestamp;
	}
	
	/**
	 * 
	 * Starts the Transaction manager
	 * @param none
	 * @return none
	 * 
	 */
	public void start() {
		try {
			while(true){
				String readLine = br.readLine();
				if (readLine == null)
					break;
				if (readLine.startsWith("//") || readLine.startsWith("/"))
					continue;
				if (!readLine.isEmpty())
					parseLine(readLine);				
				_timestamp++;
			}
			br.close();
		}
		catch (Exception e) {
			System.err.println("TM -> start(): "+e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * parse the line received in input, processes it and calls the respective method 
	 * @param line
	 * @return none
	 * 
	 */
	private void parseLine(String line) {
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
				case "end" : endTransaction(args);
							break;
				case "fail" : failSite(Integer.parseInt(args));
							break;
				case "recover" : recoverSite(Integer.parseInt(args));
							break;
				case "dump" : dump(args);
							break;
				case "querystate" :
							break;
				case "exit" : System.out.println("Exiting from Database");	
						  System.exit(0);
						  break;
				default : System.out.println("Please give the right command");
						  break; 	
			}
		}
	}

	
	/**
	 * 
	 * Method to provide a dump of a variable or a site
	 * @param sitesId or variableId
	 * @return none
	 * 
	 */
	private void dump(String args) {
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

	/**
	 * 
	 * Method to provide a dump of a variable
	 * @param variableId
	 * @return none
	 * 
	 */
	private void dumpVariable(int variable) {
		List<Integer> siteList = _variableMap.get(variable);
		for (Integer site : siteList) {
			DatabaseManager dm = _databaseManagers.get(site-1);
			System.out.println("---------- Site "+dm.getSiteIndex()+" ----------");
			dm.dumpVariable(variable);
			System.out.println();
		}
	}

	/**
	 * 
	 * Method to provide a dump of a site
	 * @param siteId
	 * @return none
	 * 
	 */	
	private void dumpSite(int site) {
		for(DatabaseManager dm : _databaseManagers){
			if(dm.getSiteIndex() == site){
				System.out.println("---------- Site "+dm.getSiteIndex()+" ----------");
				dm.dumpSite();
				System.out.println();
			}
		}
	}

	/**
	 * 
	 * Method to recover a site
	 * @param siteId
	 * @return none
	 * 
	 */	
	private void recoverSite(int site) {
		_databaseManagers.get(site-1).recover();
		System.out.println("Site "+site+" recovered.");
		System.out.println();
	}
	
	/**
	 * 
	 * Method to fail a site
	 * @param siteId
	 * @return none
	 * 
	 */	
	private void failSite(int site) {
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
			System.out.println();
		}
		_databaseManagers.get(site-1).fail();
		System.out.println("Site "+site+" failed");
		System.out.println();
	}

	/**
	 * 
	 * Method to abort a transaction
	 * @param siteId
	 * @return none
	 * 
	 */	
	private void abort(Integer trans) {
		System.out.println("Abort T"+trans);
		for(DatabaseManager dm : _databaseManagers){
			dm.abortTransaction(trans);
		}
		_transLockTransMap.remove(trans);
		_abortedTransactions.add(trans);
		Queue<Operation> queue = _transOperationMap.get(trans);
		for(Operation op : queue) {
			_waitingOperations.remove(op);
		}
		_transOperationMap.remove(trans);
		_transLockTransMap.remove(trans);
		checkToReleaseWaitingTransactions();
	}
	
	/**
	 * 
	 * Method to end a transaction
	 * @param transId
	 * @return none
	 * 
	 */	
	private void endTransaction(String args) {
		Integer transaction = Integer.parseInt(args.substring(1));
		if(_abortedTransactions.contains(transaction)) {
			System.out.println("Transation"+ args +" already aborted");
			return;
		}
		
		Transaction tTrans = _transactions.get(transaction);
		if(tTrans.get_type() == edu.nyu.adbms.Transaction.Type.RO) {
			Queue<Operation> opQueue = _transOperationMap.get(transaction);
			for(Operation op : opQueue) {
				List<List<Data>> everySiteDataList = _transSiteDataList.get(transaction);
				for(List<Data> dataList : everySiteDataList){
					for(Data data : dataList) {
						if(op.get_varIndex() == data.get_index()){
							if(data.get_value() != 0){
								System.out.println("variable x"+data.get_index()+" from T"+
										transaction+" has value "+data.get_value());
							}
							else {
								int tmp = 0;
								List<Integer> siteList = _variableMap.get(op.get_varIndex());
								for(Integer i : siteList) {
									DatabaseManager dm = _databaseManagers.get(i-1);
									List<Data> tempDataList =  dm.get_variableList();
									for(Data tempData : tempDataList) {
										if( op.get_varIndex() == tempData.get_index()){
											System.out.println("variable x"+data.get_index()+" from T"+
													transaction+" has value "+data.get_value());
											tmp = 1;
											break;
										}
									}
									if(tmp == 1)
										break;
								}
							}
						}
					}
				}
			}
		}
		else {
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
	}

	/**
	 * 
	 * Method which checks where if any operations can be released from waiting list
	 * @param none
	 * @return none
	 * 
	 */	
	private void checkToReleaseWaitingTransactions() {
		Operation tempOperation;
		Queue<Operation> waitingOperations = new LinkedList<Operation>();
		waitingOperations.addAll(_waitingOperations);
		while(waitingOperations.peek() != null) {
			tempOperation = waitingOperations.poll();
			_waitingOperations.poll();
			if(tempOperation.get_type() == Type.READ){
				read(tempOperation);
			}
			else {
				write(tempOperation);
			}
		}
	}
	
	/**
	 * 
	 * Method to parse only read and write transactions
	 * @param Read and write operation arguments
	 * @return none
	 * 
	 */
	private void parseReadWrite(String args) {
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

	/**
	 * 
	 * Method to run the write operation
	 * @param write operation
	 * @return none
	 * 
	 */
	private void write(Operation op) {
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
			List<Integer> transList = getWhichTransIsNotGivingLock(op);
			_transLockTransMap.put(op.get_transactionId(), transList);
			checkforDeadLock();
		}
		else {
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
	
	/**
	 * 
	 * Method to check for deadlock between waiting transactions using cycle deadlock
	 * @param none
	 * @return none
	 * 
	 */
	private void checkforDeadLock() {
		Graph<Integer> graph = new Graph<Integer>(true);
		Iterator<Entry<Integer, List<Integer>>> it = _transLockTransMap.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<Integer, List<Integer>> pair = (Map.Entry<Integer, List<Integer>>) it.next();
			if(pair.getValue().size() == 1) {
				graph.addEdge(pair.getKey(), pair.getValue().get(0));
			}
		}
		boolean isCycle = graph.hasCycle(graph);
		if(isCycle){
			System.out.println("Cycle Detected");
			Integer youngTrans = 0, youngestTimestamp = 0;
			Iterator<Entry<Integer, List<Integer>>> itr = _transLockTransMap.entrySet().iterator();
			while(itr.hasNext()) {
				Map.Entry<Integer, List<Integer>> pair = (Map.Entry<Integer, List<Integer>>) itr.next();
				Transaction trans = _transactions.get(pair.getKey());
				if(trans.get_timestamp() > youngestTimestamp){
					youngestTimestamp = trans.get_timestamp();
					youngTrans = trans.get_transId();
				}
			}
			abort(youngTrans);
		}
	}
	
	/**
	 * 
	 * Method to get list of transactions because of which a transaction is not getting lock
	 * @param operation which has been put to waiting
	 * @return none
	 * 
	 */
	private List<Integer> getWhichTransIsNotGivingLock(Operation op) {
		List<Integer> transList = new ArrayList<Integer>();
		for(DatabaseManager dm : _databaseManagers) {
			Map<Integer,List<Integer>> variableTransMap = dm.get_variableTransLockMap();
			if(variableTransMap.containsKey(op.get_varIndex())){
				List<Integer> list = variableTransMap.get(op.get_varIndex());
				for(Integer i : list) {
					if(!transList.contains(i))
						transList.add(i);
				}
			}
		}
		return transList;
	}

	/**
	 * 
	 * Method to run the write operation
	 * @param Read operation
	 * @return none
	 * 
	 */
	private void read(Operation op) {
		boolean result = false;
		if(_abortedTransactions.contains(op.get_transactionId()))
			return;
		if(_transactions.get(op.get_transactionId()).get_type() == Transaction.Type.RO) {
			//
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
			List<Integer> transList = getWhichTransIsNotGivingLock(op);
			_transLockTransMap.put(op.get_transactionId(), transList);
			checkforDeadLock();
		}
		else {
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

	/**
	 * 
	 * Method to parse only read and write transactions
	 * @param Read and write operation arguments
	 * @return none
	 * 
	 */
	private void beginTransaction(String type, int transId) {
		if(type == "RW"){
			_transactions.put(transId, new Transaction(transId, _timestamp, Transaction.Type.RW));
		}
		else {
			List<List<Data>> everySiteDataList = new ArrayList<List<Data>>();
			_transactions.put(transId, new Transaction(transId, _timestamp, Transaction.Type.RO));
			for(DatabaseManager dm : _databaseManagers) {
				everySiteDataList.add(dm.siteSnapShot());
			}
			_transSiteDataList.put(transId, everySiteDataList);
		}
	}
}
