/**
 * 
 */
package edu.nyu.adbms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import edu.nyu.adbms.Lock.LockType;
import edu.nyu.adbms.Operation.Type;
/**
 * @author pratyush1
 *
 */
public class DatabaseManager {
	
	private boolean _siteStatus;
	private int _siteIndex;
	private int _lastFailTime;
	private List<Data> _variableList;
	private Map<Integer, Queue<Operation>> _allowedTransactions;
	private Map<Integer, List<Lock>> _lockTable;
	
	public DatabaseManager(int siteIndex) {
		_siteStatus = true;
		_siteIndex = siteIndex;
		_lastFailTime = -1;
		_variableList = new ArrayList<Data>();
		_allowedTransactions = new HashMap<Integer, Queue<Operation>>();
		_lockTable = new HashMap<Integer, List<Lock>>();
		init();
	}
	
	public void init() {
		/*for (int i = 1; i <= 20; i++) {
			if(i % 2 == 0){
				Data d = new Data(10 * i, i);
				_variableList.add(d);
			}
			if ((1 + i % 10) == _siteIndex) {
				Data d = new Data(10 * i, i);
				_variableList.add(d);
			}
		}*/
		for (int i = 1; i <= 20; i++) {
			List<Data> dataList = new ArrayList<Data>();
		    if (i % 2 == 0 || (1 + i % 10) == _siteIndex) {
		    	_variableList.add(new Data(10 * i, i));
		    }
		}
	}

	/**
	 * @return the _siteStatus
	 */
	public boolean getSiteStatus() {
		return _siteStatus;
	}

	/**
	 * @param _siteStatus the _siteStatus to set
	 */
	public void setSiteStatus(boolean _siteStatus) {
		this._siteStatus = _siteStatus;
	}

	/**
	 * @return the _variableList
	 */
	public List<Data> get_variableList() {
		return _variableList;
	}

	/**
	 * @param _variableList the _variableList to set
	 */
	public void set_variableList(List<Data> _variableList) {
		this._variableList = _variableList;
	}

	public List<Operation> getAllowedOperations() {
		// TODO Auto-generated method stub
		List<Operation> opList = new ArrayList<Operation>();
		Iterator<Entry<Integer, Queue<Operation>>> it = _allowedTransactions.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<Integer, Queue<Operation>> pair = (Map.Entry<Integer, Queue<Operation>>)it.next();
	        Queue<Operation> q = pair.getValue();
	        opList.addAll(q);
	    }
		return opList;
	}

	public void fail() {
		// TODO Auto-generated method stub
		setSiteStatus(false);
		_lockTable.clear();
		_allowedTransactions.clear();
	}

	public void recover() {
		// TODO Auto-generated method stub
		for(int i = 0; i < _variableList.size(); i++){
			Data data = _variableList.get(i);
			data.set_accessibleForRead(false);
			data.set_value(data.get_lastCommitValue());
			_variableList.set(i, data);
		}
		_allowedTransactions = new HashMap<Integer, Queue<Operation>>();
		_lockTable = new HashMap<Integer, List<Lock>>();
	}

	public int getSiteIndex() {
		// TODO Auto-generated method stub
		return this._siteIndex;
	}

	public void dumpSite() {
		// TODO Auto-generated method stub
		for(int i=0; i < _variableList.size(); i++) {
			Data data = _variableList.get(i);
			System.out.print("Variable x"+data.get_index()+" : "+data.get_value());
			System.out.println();
		}
	}

	public void dumpVariable(int variable) {
		// TODO Auto-generated method stub
		for(Data d : _variableList) {
			if(d.get_index() == variable) {
				System.out.println("value : "+d.get_value());
			}
		}
	}

	public void abortTransaction(Integer trans) {
		// TODO Auto-generated method stub
		if(_lockTable.containsKey(trans)){
			for(Iterator<Entry<Integer, List<Lock>>> it = _lockTable.entrySet().iterator(); it.hasNext(); ) {
			    Entry<Integer, List<Lock>> entry = it.next();
			    if(entry.getKey().equals(trans)) {
			        it.remove();
			    }
			}
			
			for(Iterator<Entry<Integer, Queue<Operation>>> it = _allowedTransactions.entrySet().iterator(); it.hasNext(); ) {
			    Entry<Integer, Queue<Operation>> entry = it.next();
			    if(entry.getKey().equals(trans)) {
			        it.remove();
			    }
			}
		}
	}

	public boolean acquireLock(Operation op) {
		// TODO Auto-generated method stub
		List<Lock> locksOnVar = _lockTable.get(op.get_varIndex());
		if(op.get_type() == Type.READ) {
			if(locksOnVar == null) {
				List<Lock> newLockList = new ArrayList<Lock>();
				newLockList.add(new Lock(op.get_transactionId(), LockType.READ));
				_lockTable.put(op.get_varIndex(), newLockList);
				addToAllowedTransactions(op);
				return true;
			}
			else {
				List<Lock> lockList = _lockTable.get(op.get_varIndex());
				for(Lock templock : lockList) {
					if (templock.getType() == Lock.LockType.WRITE)
						return false;
				}
				lockList.add(new Lock(op.get_transactionId(), LockType.READ));
				addToAllowedTransactions(op);
				return true;
			}
		}
		else {
			if(locksOnVar == null){
				List<Lock> newLockList = new ArrayList<Lock>();
				newLockList.add(new Lock(op.get_transactionId(), LockType.WRITE));
				_lockTable.put(op.get_varIndex(), newLockList);
				addToAllowedTransactions(op);
				return true;
			}
			return false;
		}
		
	}

	private void addToAllowedTransactions(Operation op) {
		// TODO Auto-generated method stub
		if(_allowedTransactions.containsKey(op.get_transactionId())){
			Queue<Operation> operations = _allowedTransactions.get(op.get_transactionId());
			operations.add(op);
			_allowedTransactions.put(op.get_transactionId(), operations);
		}
		else {
			Queue<Operation> operations = new LinkedList<Operation>();
			operations.add(op);
			_allowedTransactions.put(op.get_transactionId(), operations);
		}
	}
}
