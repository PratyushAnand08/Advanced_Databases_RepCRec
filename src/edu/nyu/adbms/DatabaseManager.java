/**
 * 
 */
package edu.nyu.adbms;

<<<<<<< HEAD
=======
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

>>>>>>> repcrec
/**
 * @author pratyush1
 *
 */
public class DatabaseManager {
<<<<<<< HEAD

=======
	
	private boolean _siteStatus;
	private int _siteIndex;
	private int _lastFailTime;
	private Map<Integer, List<Data>> _dataMap = new HashMap<Integer, List<Data>>();
	private Map<Integer, Data> _uncommitDataMap = new HashMap<Integer, Data>();
	private Map<Integer, List<Lock>> _lockTable = new HashMap<Integer, List<Lock>>();
	private Set<Integer> _accessedTransactions = new HashSet<Integer>();
	
	public DatabaseManager(int siteIndex) {
		_siteStatus = true;
		_siteIndex = siteIndex;
		_lastFailTime = -1;
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

	public List<Integer> getAllAccessedTransactions() {
		// TODO Auto-generated method stub
		return null;
	}

	public void fail() {
		// TODO Auto-generated method stub
		
	}

	public void recover() {
		// TODO Auto-generated method stub
		
	}

	public int getSiteIndex() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void dumpSite() {
		// TODO Auto-generated method stub
		
	}

	public void dumpVariable(int variable) {
		// TODO Auto-generated method stub
		
	}
>>>>>>> repcrec
}
