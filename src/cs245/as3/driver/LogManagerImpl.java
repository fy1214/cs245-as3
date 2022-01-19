package cs245.as3.driver;

import java.nio.ByteBuffer;

import cs245.as3.interfaces.LogManager;

/**
 * DO NOT MODIFY THIS FILE IN THIS PACKAGE **
 * Make this an interface
 */
public class LogManagerImpl implements LogManager {
	private ByteBuffer log;	 //日志缓存
	private int logSize;	//已写日志大小
	private int logTruncationOffset;
	private int Riop_counter;	//读日志操作数
	private int Wiop_counter;	//写日志操作数
	private int nIOSBeforeCrash;	//执行多少次后系统崩溃
	private boolean serveRequests;	//表示能否被继续使用，false表示系统崩溃

	// 1 GB
	private static final int BUFSIZE_START = 1000000000;

	// 128 byte max record size
	private static final int RECORD_SIZE = 128;		//每条记录大小

	public LogManagerImpl() {
		log = ByteBuffer.allocate(BUFSIZE_START);
		logSize = 0;
		serveRequests = true;
	}

	/* **** Public API **** */

	public int getLogEndOffset() {
		return logSize;
	}

	public byte[] readLogRecord(int position, int size) throws ArrayIndexOutOfBoundsException {
		checkServeRequest();
		if ( position < logTruncationOffset || position+size > getLogEndOffset() ) {
			throw new ArrayIndexOutOfBoundsException("Offset " + (position+size) + "invalid: log start offset is " + logTruncationOffset +
					", log end offset is " + getLogEndOffset());
		}

		if ( size > RECORD_SIZE ) {
			throw new IllegalArgumentException("Record length " + size +
					" greater than maximum allowed length " + RECORD_SIZE);
		}

		byte[] ret = new byte[size];
		log.position(position);
		log.get(ret);
		Riop_counter++;
		return ret;
	}

	//Returns the length of the log before the append occurs, atomically
	//返回发生追加之前日志的长度（原子性）
	public int appendLogRecord(byte[] record) {
		checkServeRequest();
		if ( record.length > RECORD_SIZE ) {
			throw new IllegalArgumentException("Record length " + record.length +
					" greater than maximum allowed length " + RECORD_SIZE);
		}

		synchronized(this) {
			Wiop_counter++;

			//position变量跟踪已经写了多少数据。更准确地说，它指定了下一个字节将放到数组的哪一个元素中。
			//logSize表示已经写的日志的大小
			log.position(logSize);

			for ( int i = 0; i < record.length; i++ ) {
				log.put(record[i]);
			}

			int priorLogSize = logSize;

			logSize += record.length;
			return priorLogSize;
		}
	}

	//获得日志检查点
	public int getLogTruncationOffset() {
		return logTruncationOffset;
	}

	//设置日志检查点
	public void setLogTruncationOffset(int logTruncationOffset) {
		if (logTruncationOffset > logSize || logTruncationOffset < this.logTruncationOffset) {
			throw new IllegalArgumentException();
		}

		this.logTruncationOffset = logTruncationOffset;
	}


	/* **** For testing only **** */
	
	protected class CrashException extends RuntimeException {
		
	}
	/**
	 * We use this to simulate the log becoming inaccessible after a certain number of operations, 
	 * for the purposes of simulating crashes.
	 * 为了模拟崩溃，我们使用它来模拟在一定数量的操作之后日志变得不可访问。
	 */
	private void checkServeRequest() {
		if (nIOSBeforeCrash > 0) {
			nIOSBeforeCrash--;
			if (nIOSBeforeCrash == 0) {
				serveRequests = false;
			}
		}
		if (!serveRequests) {
			//Crash!
			throw new CrashException();
		}
		//如果[boolean表达式]为true，则程序继续执行。
		//如果为false，则程序抛出AssertionError，并终止执行。
   		assert(!Thread.interrupted()); //Cooperate with timeout:
	}

	protected int getIOPCount() {
		return Riop_counter + Wiop_counter;
	}

	protected void crash() {
		log.clear();
	}

	protected void stopServingRequestsAfterIOs(int nIOsToServe) {
		nIOSBeforeCrash = nIOsToServe;
	}

	protected void resumeServingRequests() {
		serveRequests = true;
		nIOSBeforeCrash = 0;
	}
}