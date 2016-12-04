/**
 * 
 */
package edu.nyu.adbms;

<<<<<<< HEAD
=======
import java.util.Date;

>>>>>>> repcrec
/**
 * @author pratyush1
 *
 */
public class Operation {
	
	public static enum Type {
	    READ, WRITE,
	  };

	  private int _transactionId;
	  private Type _type;
	  private int _varIndex;
	  private int _writeValue = 0;
<<<<<<< HEAD
	  private int _timestamp;
=======
	  private Date _timestamp;
>>>>>>> repcrec
	/**
	 * @param _transactionId
	 * @param _type
	 * @param _varIndex
	 * @param _timestamp
	 */
<<<<<<< HEAD
	public Operation(int _transactionId, Type _type, int _varIndex, int _timestamp) {
=======
	public Operation(int _transactionId, Type _type, int _varIndex, Date _timestamp) {
>>>>>>> repcrec
		this._transactionId = _transactionId;
		this._type = _type;
		this._varIndex = _varIndex;
		this._timestamp = _timestamp;
	}
	/**
	 * @param _transactionId
	 * @param _type
	 * @param _varIndex
	 * @param _writeValue
	 * @param _timestamp
	 */
<<<<<<< HEAD
	public Operation(int _transactionId, Type _type, int _varIndex, int _writeValue, int _timestamp) {
=======
	public Operation(int _transactionId, Type _type, int _varIndex, int _writeValue, Date _timestamp) {
>>>>>>> repcrec
		this._transactionId = _transactionId;
		this._type = _type;
		this._varIndex = _varIndex;
		this._writeValue = _writeValue;
		this._timestamp = _timestamp;
	}
	/**
	 * @return the _transactionId
	 */
	public int get_transactionId() {
		return _transactionId;
	}
	/**
	 * @return the _type
	 */
	public Type get_type() {
		return _type;
	}
	/**
	 * @return the _varIndex
	 */
	public int get_varIndex() {
		return _varIndex;
	}
	/**
	 * @return the _writeValue
	 */
	public int get_writeValue() {
		return _writeValue;
	}
	/**
	 * @return the _timestamp
	 */
<<<<<<< HEAD
	public int get_timestamp() {
=======
	public Date get_timestamp() {
>>>>>>> repcrec
		return _timestamp;
	}
	/**
	 * @param _transactionId the _transactionId to set
	 */
	public void set_transactionId(int _transactionId) {
		this._transactionId = _transactionId;
	}
	/**
	 * @param _type the _type to set
	 */
	public void set_type(Type _type) {
		this._type = _type;
	}
	/**
	 * @param _varIndex the _varIndex to set
	 */
	public void set_varIndex(int _varIndex) {
		this._varIndex = _varIndex;
	}
	/**
	 * @param _writeValue the _writeValue to set
	 */
	public void set_writeValue(int _writeValue) {
		this._writeValue = _writeValue;
	}
	/**
	 * @param _timestamp the _timestamp to set
	 */
<<<<<<< HEAD
	public void set_timestamp(int _timestamp) {
		this._timestamp = _timestamp;
	}
=======
	public void set_timestamp(Date _timestamp) {
		this._timestamp = _timestamp;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Operation [_transactionId=" + _transactionId + ", _type=" + _type + ", _varIndex=" + _varIndex
				+ ", _writeValue=" + _writeValue + ", _timestamp=" + _timestamp + "]";
	}
>>>>>>> repcrec
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
<<<<<<< HEAD
		result = prime * result + _timestamp;
=======
		result = prime * result + ((_timestamp == null) ? 0 : _timestamp.hashCode());
>>>>>>> repcrec
		result = prime * result + _transactionId;
		result = prime * result + ((_type == null) ? 0 : _type.hashCode());
		result = prime * result + _varIndex;
		result = prime * result + _writeValue;
		return result;
	}
<<<<<<< HEAD
=======
	
>>>>>>> repcrec
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
		if (!(obj instanceof Operation)) {
			return false;
		}
		Operation other = (Operation) obj;
<<<<<<< HEAD
		if (_timestamp != other._timestamp) {
=======
		if (_timestamp == null) {
			if (other._timestamp != null) {
				return false;
			}
		} else if (!_timestamp.equals(other._timestamp)) {
>>>>>>> repcrec
			return false;
		}
		if (_transactionId != other._transactionId) {
			return false;
		}
		if (_type != other._type) {
			return false;
		}
		if (_varIndex != other._varIndex) {
			return false;
		}
		if (_writeValue != other._writeValue) {
			return false;
		}
		return true;
<<<<<<< HEAD
	}
	
	
=======
	}	
>>>>>>> repcrec
}
