/**
 * 
 */
package edu.nyu.adbms;

/**
 * @author Pratyush Anand (pa1139)
 * @author Arpit Saini (ads745) 
 *
 */
public class Lock {

	private enum LockType { READ, WRITE };
	private int _transId;
	private LockType _type;
	
	/**
	 * @param transId
	 * @param type
	 */
	public Lock(int transId, LockType type) {
		super();
		_transId = transId;
		_type = type;
	}

	/**
	 * @return the _transId
	 */
	public int getTransId() {
		return _transId;
	}

	/**
	 * @return the _type
	 */
	public LockType getType() {
		return _type;
	}

	/**
	 * @param transId the _transId to set
	 */
	public void setTransId(int transId) {
		_transId = transId;
	}

	/**
	 * @param type the _type to set
	 */
	public void setType(LockType type) {
		_type = type;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + _transId;
		result = prime * result + ((_type == null) ? 0 : _type.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Lock)) {
			return false;
		}
		Lock other = (Lock) obj;
		if (_transId != other._transId) {
			return false;
		}
		if (_type != other._type) {
			return false;
		}
		return true;
	}
	
	public void escalateLock() {
	    _type = LockType.WRITE;
	  }
}
