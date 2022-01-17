package cs245.as3.interfaces;

import java.util.HashMap;

public interface StorageManager {
	public static class TaggedValue {
		public final long tag;
		public final byte[] value;
		
		public TaggedValue(long tag, byte[] value) {
			this.tag = tag;
			this.value = value;
		}
	}

	/**
	 * Reads the current persisted table of values. Should only be called during recovery.
	 * @return a copy of the data (will be empty if no values are stored)
	 * 读取当前持久化的值表。应仅在恢复期间调用。
	 */
	public HashMap<Long, TaggedValue> readStoredTable();

	/**
	 * Queues a write to storage with the given key, tag, and value.
	 * 使用给定的键、标记和值对写入存储器进行排队。
	 */
	public void queueWrite(long key, long tag, byte[] value);
}
