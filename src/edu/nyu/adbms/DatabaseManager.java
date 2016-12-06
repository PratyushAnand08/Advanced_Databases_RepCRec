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
import java.util.concurrent.CopyOnWriteArrayList;

import edu.nyu.adbms.Lock.LockType;
import edu.nyu.adbms.Operation.Type;
/**
 * @author Pratyush Anand (pa1139)
 * @author Arpit Saini (ads745)
 *
 */
public class DatabaseManager {
	
	private boolean _siteStatus;
	private int _siteIndex;
	private List<Data> _variableList;
	private Map<Integer, Queue<Operation>> _allowedTransactions; //(TransId, List(Operation))
	private Map<Integer, List<Lock>> _lockTable; //vardId,List(Locks)
	private Map<Integer, List<Integer>> _variableTransLockMap; //varId, List(Trans)
	
	public DatabaseManager(int siteIndex) {
		_siteStatus = true;
		_siteIndex = siteIndex;
		_variableList = new ArrayList<Data>();
		_allowedTransactions = new HashMap<Integer, Queue<Operation>>();
		_lockTable = new HashMap<Integer, List<Lock>>();
		_variableTransLockMap = new HashMap<Integer, List<Integer>>();
		init();
	}
	
	public void init() {
		for (int i = 1; i <= 20; i++) {
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

	/**
	 * @return the _variableTransLockMap
	 */
	public Map<Integer, List<Integer>> get_variableTransLockMap() {
		return _variableTransLockMap;
	}

	/**
	 * @param _variableTransLockMap the _variableTransLockMap to set
	 */
	public void set_variableTransLockMap(Map<Integer, List<Integer>> _variableTransLockMap) {
		this._variableTransLockMap = _variableTransLockMap;
	}

	public List<Operation> getAllowedOperations() {
		// TODO Auto-generated method stub
		List<Operation> opList = new CopyOnWriteArrayList<Operation>();
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
		_variableTransLockMap.clear();
	}

	public void recover() {
		// TODO Auto-generated method stub
		for(int i = 0; i < _variableList.size(); i++){
			Data data = _variableList.get(i);
			if(data.get_index() % 2 == 0)
				data.set_accessibleForRead(false);
			else
				data.set_accessibleForRead(true);
			data.set_value(data.get_lastCommitValue());
			_variableList.set(i, data);
		}
		this.setSiteStatus(true);
		_allowedTransactions = new HashMap<Integer, Queue<Operation>>();
		_lockTable = new HashMap<Integer, List<Lock>>();
		_variableTransLockMap = new HashMap<Integer, List<Integer>>();
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
		_allowedTransactions.remove(trans);
		Iterator<Entry<Integer, List<Integer>>> it = _variableTransLockMap.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<Integer, List<Integer>> pair = (Map.Entry<Integer, List<Integer>>) it.next();
			if(pair.getValue().contains(trans)) {
				pair.getValue().remove(trans);
			}
			if(pair.getValue().size() == 0) {
				_variableTransLockMap.remove(pair.getKey());
			}
			else {
				_variableTransLockMap.put(pair.getKey(), pair.getValue());
			}
		}
		Iterator<Entry<Integer, List<Lock>>> itr = _lockTable.entrySet().iterator();
		while(itr.hasNext()){
			Map.Entry<Integer, List<Lock>> anotherPair = (Map.Entry<Integer, List<Lock>>) itr.next();
			List<Lock> tempLockList = new CopyOnWriteArrayList<Lock>();
			tempLockList = anotherPair.getValue();
			for(Lock lock : anotherPair.getValue()) {
				if(lock.getTransId() == trans){
					tempLockList.remove(lock);
				}
			}
			if(anotherPair.getValue().size() == 0) {
				_lockTable.remove(anotherPair.getKey());
			}
			else {
				_lockTable.put(anotherPair.getKey(), tempLockList);
			}
		}
	}

	public boolean acquireLock(Operation op) {
		// TODO Auto-generated method stub
		List<Lock> locksOnVar = _lockTable.get(op.get_varIndex());
		List<Integer> transList = _variableTransLockMap.get(op.get_varIndex());
		int setLock = 0;
		if(op.get_type() == Type.READ) {
			if(locksOnVar == null) {
				List<Lock> newLockList = new CopyOnWriteArrayList<Lock>();
				if (op.get_writeValue() != 0)
					newLockList.add(new Lock(op.get_transactionId(), LockType.WRITE));
				else
					newLockList.add(new Lock(op.get_transactionId(), LockType.READ));
				_lockTable.put(op.get_varIndex(), newLockList);
				if(transList == null){
					transList = new CopyOnWriteArrayList<Integer>();
				}
				transList.add(op.get_transactionId());
				_variableTransLockMap.put(op.get_varIndex(), transList);
				addToAllowedTransactions(op);
				return true;
			}
			else {
				for(Lock templock : locksOnVar) {
					if (templock.getType() == LockType.WRITE) {
						setLock = 0;
						break;
					}
					else {
						setLock = 1;
						break;
					}
				}
				if(setLock == 1) {
					locksOnVar.add(new Lock(op.get_transactionId(), LockType.READ));
					_lockTable.put(op.get_varIndex(), locksOnVar);
					addToAllowedTransactions(op);
					if(transList == null){
						transList = new CopyOnWriteArrayList<Integer>();
					}
					transList.add(op.get_transactionId());
					_variableTransLockMap.put(op.get_varIndex(), transList);
					return true;
				}
				return false;
			}
		}
		else {
			if(locksOnVar == null){
				List<Lock> newLockList = new CopyOnWriteArrayList<Lock>();
				newLockList.add(new Lock(op.get_transactionId(), LockType.WRITE));
				_lockTable.put(op.get_varIndex(), newLockList);
				addToAllowedTransactions(op);
				if(transList == null){
					transList = new CopyOnWriteArrayList<Integer>();
				}
				transList.add(op.get_transactionId());
				_variableTransLockMap.put(op.get_varIndex(), transList);
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

	public boolean isVarAccessible(int get_varIndex) {
		// TODO Auto-generated method stub
		for(Data d : _variableList) {
			if (d.get_index() == get_varIndex && d.is_accessible()){
				return true;
			}
		}
		return false;
	}

	public void commit(Operation op) {
		// TODO Auto-generated method stub
		System.out.println("From Site "+this._siteIndex);
		if(op.get_type() == Type.READ){
			for(Data data : _variableList) {
				if(data.get_index() == op.get_varIndex()) {
					System.out.println("variable x"+op.get_varIndex()+" from T"+
							op.get_transactionId()+" has value "+data.get_value());
					break;
				}
			}
			List<Lock> lockList = _lockTable.get(op.get_varIndex());
			lockList.remove(0);
			if(!lockList.isEmpty()) {
				_lockTable.put(op.get_varIndex(), lockList);
			}
			else {
				_lockTable.remove(op.get_varIndex());
			}
			Queue<Operation> operation = _allowedTransactions.get(op.get_transactionId());
			operation.poll();
			if(!operation.isEmpty()) {
				_allowedTransactions.put(op.get_transactionId(), operation);
			}
			else {
				_allowedTransactions.remove(op.get_transactionId());
			}
		}
		else {
			//Data data = _variableList.get(op.get_varIndex()-1);
			for(Data data : _variableList){
				if(data.get_index() == op.get_varIndex()){
					data.set_lastCommitValue(data.get_value());
					data.set_value(op.get_writeValue());
					if(!data.is_accessible()){
						data.set_accessibleForRead(true);
					}
					System.out.println("New value for variable x"+op.get_varIndex()+
							" is "+data.get_value()+" from T"+op.get_transactionId());
				}
			}
			_lockTable.remove(op.get_varIndex());
			Queue<Operation> operation = _allowedTransactions.get(op.get_transactionId());
			operation.poll();
			if(!operation.isEmpty()) {
				_allowedTransactions.put(op.get_transactionId(), operation);
			}
			else {
				_allowedTransactions.remove(op.get_transactionId());
			}
		}
	}

	public List<Data> siteSnapShot() {
		// TODO Auto-generated method stub
		List<Data> tempDataList = new ArrayList<Data>();
		for(Data data : _variableList) {
			Data tmpData = new Data(data.get_value(), data.get_index());
			List<Integer> tempTransList = _variableTransLockMap.get(data.get_index());
			if(tempTransList != null) {
				for(Integer trans : tempTransList) {
					Iterator<Entry<Integer, Queue<Operation>>> it = _allowedTransactions.entrySet().iterator();
					while(it.hasNext()){
						Map.Entry<Integer, Queue<Operation>> pair = (Map.Entry<Integer, Queue<Operation>>) it.next();
						if(trans == pair.getKey()) {
							for(Operation op : pair.getValue()){
								if(op.get_type() == Type.WRITE) {
									tmpData.set_value(0);
								}
							}
						}
					}
				}
			}
			tempDataList.add(tmpData);
		}
		return tempDataList;
	}
}
