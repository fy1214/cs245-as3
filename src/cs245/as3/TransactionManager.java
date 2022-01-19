package cs245.as3;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * 我们在下面提供的实现执行原子事务，但这些更改并不持久。
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 *
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 * 相同的数据结构（带有附加字段）并使用相同的缓冲写入策略，直到提交。
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 * 您的实现不需要是线程安全的，即TransactionManager的任何方法都不会被并发调用。
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 * 您可以假设构造函数和initAndRecover（）都是在任何其他方法之前调用的。
 */

public class TransactionManager {
	//每条日志记录长度117
	static class Record {
		long TXid;
		long key;
		byte[] value = new byte[117 - 2 * Long.BYTES];
		static int len = 117;

		public Record() {
		}

		Record(long TXid, long key, byte[] _value) {
			this.TXid = TXid;
			this.key = key;
			System.arraycopy(_value, 0, this.value, 1, _value.length);
			value[0] = (byte) _value.length;
		}

		//序列化记录
		public byte[] serialize() {
			ByteBuffer ret = ByteBuffer.allocate(len);
			ret.putLong(TXid);
			ret.putLong(key);
			ret.put(value);
			return ret.array();
		}

		//反序列化记录
		public static Record deserialize(byte[] b) {
			ByteBuffer bb = ByteBuffer.wrap(b);
			long TXid = bb.getLong();
			long key = bb.getLong();
			byte[] value = new byte[len - 2 * Long.BYTES];
			bb.get(value);
			Record record = new Record();
			record.TXid = TXid;
			record.key = key;
			record.value = value;
			return record;
		}

		public byte[] trueValue() {
			byte[] res = new byte[value[0]];
			for (int i = 0; i < res.length; i++) {
				res[i] = value[i + 1];
			}
			return res;
		}
	}

	/**
	  * Holds the latest value for each key.
	  * 对于每个key，持有最新的值
	  */
	private HashMap<Long, TaggedValue> latestValues;

	/**
	 * 数据持久化时checkpoint检查，每次写入sm时都必须同时写入checkPersist，检查数据是否被持久化
	 */
	//private HashMap<Long, Integer> checkPersist;
	private LRU checkPersist;

	/**
	 * Hold on to writesets until commit.
	 * 对每一个record，保持到commit
	 */
	private HashMap<Long, ArrayList<Record>> logsets;

	public TransactionManager() {
		//checkPersist = new HashMap<>();
		checkPersist = new LRU();
		logsets = new HashMap<>();
		//see initAndRecover
		latestValues = null;
	}

	private StorageManager StorageManager_listener;
	private LogManager LogManager_listener;

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		latestValues = sm.readStoredTable();
		this.StorageManager_listener = sm;
		this.LogManager_listener = lm;
		if (lm.getLogEndOffset() != 0) {
			//事务表，收集所有事物
			Map<Long, List<RecordAndOffset>> transactionTable = new HashMap<>();
			int start = lm.getLogTruncationOffset();
			int offset = lm.getLogEndOffset();
			for (int i = start; i < offset; i = i + Record.len) {
				Record r = Record.deserialize(lm.readLogRecord(i, Record.len));
				if (!transactionTable.containsKey(r.TXid)) {
					ArrayList<RecordAndOffset> records = new ArrayList<>();
					records.add(new RecordAndOffset(r, i));
					transactionTable.put(r.TXid, records);
				}else {
					transactionTable.get(r.TXid).add(new RecordAndOffset(r, i));
				}
			}

			//收集所有已提交的值中最后更新的值的记录
			Map<Long, RecordAndOffset> latestRecordList = new HashMap<>();
			for (Map.Entry<Long, List<RecordAndOffset>> entry : transactionTable.entrySet()) {
				//检查事务组是否被commit
				List<RecordAndOffset> recordsEachTXid = entry.getValue();
				RecordAndOffset commitRecord = recordsEachTXid.get(recordsEachTXid.size() - 1);
				if (commitRecord.record.key != Integer.MIN_VALUE) {
					continue;
				}

				int off = recordsEachTXid.get(0).offset;
				for (int i = recordsEachTXid.size() - 2; i >= 0; i--) {
					RecordAndOffset currRecord = recordsEachTXid.get(i);
					currRecord.offset = off;
					if (!latestRecordList.containsKey(currRecord.record.key)) {
						latestRecordList.put(currRecord.record.key, currRecord);
					}else {
						RecordAndOffset preRecord = latestRecordList.get(currRecord.record.key);
						if (preRecord.offset < currRecord.offset) {
							latestRecordList.put(currRecord.record.key, currRecord);
						}
					}
				}

			}

			//重做所有日志
			for (Map.Entry<Long, RecordAndOffset> recordOffsetEntry : latestRecordList.entrySet()) {
				RecordAndOffset r = recordOffsetEntry.getValue();
				sm.queueWrite(r.record.key, r.offset, r.record.trueValue());
				latestValues.put(r.record.key, new TaggedValue(r.offset, r.record.trueValue()));
				//checkPersist.put(r.record.key, r.offset);
				checkPersist.put(r.record.key, r.offset);
			}
		}
	}

	//带有偏移量的Record，偏移量统一记为事务开始时
	class RecordAndOffset {
		Record record;
		int offset;

		public RecordAndOffset(Record record, int offset) {
			this.record = record;
			this.offset = offset;
		}
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 * 表示开始新事务。我们将保证txID始终增加（即使在发生碰撞时）
	 *
	 */
	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this 未针对非持久事务实现
		ArrayList<Record> transaction = new ArrayList<>();
		logsets.put(txID, transaction);
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 * 返回任意事务最进提交的值
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read() 
	 * calls until the transaction making the write commits. For simplicity, we will not make reads 
	 * to this same key from txID itself after we make a write to the key
	 * 指示对数据库的写入。请注意，在进行写操作的事务提交之前，read（）调用不应看到此类写操作。
	 * 为简单起见，不考虑同一个事务内写了一个key，再读取相同key的情况。
	 */
	public void write(long txID, long key, byte[] value) {
		ArrayList<Record> transaction = logsets.get(txID);
		if (transaction == null) {
			return;
		}

		transaction.add(new Record(txID, key, value));
	}

	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.
	 * 提交事务，并使其写入对后续读取操作可见。
	 */
	public void commit(long txID) {
		ArrayList<Record> transaction = logsets.get(txID);

		int offset = LogManager_listener.getLogEndOffset();
		//获取事务中每个值最新的记录，并写入日志lm
		Map<Long, Record> latestRecord = new HashMap<>();
		for (int i = transaction.size() - 1; i >= 0; i--) {
			Record r = transaction.get(i);
			if (!latestRecord.containsKey(r.key)) {
				latestRecord.put(r.key, r);
				LogManager_listener.appendLogRecord(r.serialize());
			}
		}

		LogManager_listener.appendLogRecord(new Record(txID, Integer.MIN_VALUE, "commit".getBytes(StandardCharsets.UTF_8)).serialize());

		//做持久化
		for (Map.Entry<Long, Record> entry : latestRecord.entrySet()) {
			Record record = entry.getValue();
			StorageManager_listener.queueWrite(entry.getKey(), offset, record.trueValue());
			latestValues.put(entry.getKey(), new TaggedValue(offset, record.trueValue()));
			//checkPersist.put(record.key, offset);
			checkPersist.put(record.key, offset);
		}

		logsets.remove(txID);
	}

	/**
	 * Aborts a transaction.
	 * 回滚一个事务
	 */
	public void abort(long txID) {
		logsets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs\
	 * 每当队列写入变为持久时，存储管理器都会回调到此过程。
	 * 这些调用是按写入密钥的顺序进行的，除非发生崩溃，否则对于每一次这样的排队写入，都会发生一次。
	 */
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
		//int tag = Integer.MAX_VALUE;
//		checkPersist.remove(key);
//		if (checkPersist.size() == 0) {
//			LogManager_listener.setLogTruncationOffset(LogManager_listener.getLogEndOffset());
//		}else {
//			for (Map.Entry<Long, Integer> entry : checkPersist.entrySet()) {
//				tag = Math.min(tag, entry.getValue());
//			}
//			LogManager_listener.setLogTruncationOffset(tag);
//		}
		checkPersist.remove(key, persisted_tag);
		LogManager_listener.setLogTruncationOffset((int) checkPersist.getMinOffset());
	}
}

//存储持久化记录，以偏移量排序
class LRU {
	Map<Long, Node> cache;
	TreeMap<Integer, HashSet<Node>> map;
	int minOffset = Integer.MAX_VALUE;
	int maxOffset = 0;

	public LRU() {
		cache = new HashMap<>();
		map = new TreeMap<>();
	}

	public void put(long key, int _offset) {
		Node n = cache.get(key);
		if (n == null) {
			n = new Node(key, _offset);
		}else {
			HashSet<Node> set = map.get(n.offset);
			set.remove(n);
			if (set.size() == 0) {
				map.remove(n.offset);
			}
			n.offset = _offset;
		}
		cache.put(key, n);

		HashSet<Node> set = map.get(_offset);
		if (set == null) {
			set = new HashSet<>();
			map.put(_offset, set);
		}
		set.add(n);
		minOffset = map.firstKey();
		maxOffset = Math.max(maxOffset, _offset);
	}

	public void remove(long key, long _offset) {
		int offset = (int) _offset;
		Node n = cache.get(key);
		HashSet<Node> set = map.get(offset);
		if (set == null) return;
		set.remove(n);
		if (set.size() == 0) {
			map.remove(offset);
		}
		cache.remove(key);
		if (map.size() != 0) minOffset = map.firstKey();
	}

	public long getMinOffset() {
		if (map.size() != 0) {
			return minOffset;
		}else return maxOffset;
	}
}

class Node {
	long key;
	int offset;

	public Node(long key, int offset) {
		this.key = key;
		this.offset = offset;
	}
}