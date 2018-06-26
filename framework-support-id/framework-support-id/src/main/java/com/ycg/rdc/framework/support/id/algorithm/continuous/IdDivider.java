package com.ycg.rdc.framework.support.id.algorithm.continuous;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;

import com.ycg.rdc.framework.application.log.ISystemLogger;

public class IdDivider implements IIdDivider {

	private String ZK_ADDRESS = "127.0.0.1:2181";
	private String ROOT_NODE = "/idmachine";
	private int BUFFER_SIZE = 1000;
	private int RETRY_TIMES = 3;
	private int RETRY_INTERVALS = 1000;
	private int SESSION_TIMEOUT_MS = 60000;
	private int CONNECTION_TIMEOUT_MS = 60000;
	private ConcurrentHashMap<String, ArrayBlockingQueue<Long>> BUFFER_QUEUE;

	@Autowired
	private ISystemLogger logger;

	private Object syncRoot = new Object();

	static CuratorFramework client;

	public IdDivider() {
		client = CuratorFrameworkFactory.newClient(ZK_ADDRESS,
				SESSION_TIMEOUT_MS, CONNECTION_TIMEOUT_MS, new RetryNTimes(
						RETRY_TIMES, RETRY_INTERVALS));
		init();
	}

	public IdDivider(String zkAddress, String rootNode, int bufferSize,
			int retryTimes, int retryIntervals, int sessionTimeoutMs,
			int connectionTimeoutMs) {
		client = CuratorFrameworkFactory.newClient(zkAddress, sessionTimeoutMs,
				connectionTimeoutMs,
				new RetryNTimes(retryTimes, retryIntervals));
		this.ZK_ADDRESS = zkAddress;
		this.ROOT_NODE = rootNode;
		this.BUFFER_SIZE = bufferSize;
		this.RETRY_TIMES = retryTimes;
		this.RETRY_INTERVALS = retryIntervals;
		this.SESSION_TIMEOUT_MS = sessionTimeoutMs;
		this.CONNECTION_TIMEOUT_MS = connectionTimeoutMs;
		init();
	}

	@Override
	public Boolean tryToCreateScope(String scopeName, String schemeName,
			String fieldName) {
		String fullPathString = ROOT_NODE + "/" + scopeName + "/" + schemeName
				+ "/" + fieldName;
		byte[] data = ("1+1+1").getBytes();
		try {
			if (client.checkExists().forPath(fullPathString) == null) {
				client.createContainers(fullPathString);
				client.setData().forPath(fullPathString, data);
				return true;
			}
		} catch (Exception e) {
			logger.error("全局ID作用域创建失败，请检查路径是否正确。", e);
		}
		return false;
	}

	@Override
	public Boolean tryToCreateScope(String scopeName, String schemeName,
			String fieldName, Long initialValue, Long stepping) {
		String fullPathString = ROOT_NODE + "/" + scopeName + "/" + schemeName
				+ "/" + fieldName;
		byte[] data = (initialValue + "+" + initialValue + "+" + stepping)
				.getBytes();
		try {
			if (client.checkExists().forPath(fullPathString) == null) {
				client.createContainers(fullPathString);
				client.setData().forPath(fullPathString, data);
				return true;
			}
		} catch (Exception e) {
			logger.error("全局ID作用域创建失败，请检查路径是否正确。", e);
		}
		return false;
	}

	@Override
	public Boolean flushScope(String scopeName, String schemeName,
			String fieldName, Long initialValue, Long stepping) {
		String fullPathString = ROOT_NODE + "/" + scopeName + "/" + schemeName
				+ "/" + fieldName;
		byte[] data = (initialValue + "+" + initialValue + "+" + stepping)
				.getBytes();
		try {
			Stat stat = client.checkExists().forPath(fullPathString);
			if (stat != null) {
				stat = client.setData().withVersion(stat.getVersion())
						.forPath(fullPathString, data);
				return true;
			}
		} catch (Exception e) {
			logger.error("全局ID作用域重新初始化失败，请检查路径是否正确。", e);
		}
		return false;
	}

	@Override
	public void removeScope(String scopeName, String schemeName,
			String fieldName) {
		String fullPathString = ROOT_NODE + "/" + scopeName + "/" + schemeName
				+ "/" + fieldName;
		try {
			if (client.checkExists().forPath(fullPathString) != null) {
				client.delete().forPath(fullPathString);
				if (BUFFER_QUEUE.containsKey(fullPathString)) {
					BUFFER_QUEUE.remove(fullPathString);
				}
			}
		} catch (Exception e) {
			logger.error("指定的全局ID作用域删除失败，请检查路径是否正确。", e);
		}
	}

	@Override
	public void flushBuffer(String scopeName, String schemeName,
			String fieldName) {
		String fullPathString = ROOT_NODE + "/" + scopeName + "/" + schemeName
				+ "/" + fieldName;
		if (BUFFER_QUEUE == null) {
			BUFFER_QUEUE = new ConcurrentHashMap<>();
		}
		if (BUFFER_QUEUE.containsKey(fullPathString)) {
			BUFFER_QUEUE.remove(fullPathString);
		}
		BUFFER_QUEUE.put(fullPathString, new ArrayBlockingQueue<Long>(
				BUFFER_SIZE));
		bufferIds(scopeName, schemeName, fieldName);
	}

	@Override
	public void setBufferSize(String scopeName, String schemeName,
			String fieldName, Integer size) {
		String fullPathString = ROOT_NODE + "/" + scopeName + "/" + schemeName
				+ "/" + fieldName;
		if (BUFFER_QUEUE == null) {
			BUFFER_QUEUE = new ConcurrentHashMap<>();
		}
		if (BUFFER_QUEUE.containsKey(fullPathString)) {
			BUFFER_QUEUE.remove(fullPathString);
		}
		BUFFER_QUEUE.put(fullPathString, new ArrayBlockingQueue<Long>(size));
		bufferIds(scopeName, schemeName, fieldName);
	}

	@Override
	public Long nowId(String scopeName, String schemeName, String fieldName) {
		String fullPathString = ROOT_NODE + "/" + scopeName + "/" + schemeName
				+ "/" + fieldName;
		if (BUFFER_QUEUE.containsKey(fullPathString)) {
			ArrayBlockingQueue<Long> ids = BUFFER_QUEUE.get(fullPathString);
			if (ids != null && ids.size() > 0) {
				return ids.peek();
			}
		}
		return null;
	}

	@Override
	public Long next(String scopeName, String schemeName, String fieldName) {
		Long result = null;
		String fullPathString = ROOT_NODE + "/" + scopeName + "/" + schemeName
				+ "/" + fieldName;
		// 1.先检查是否存在队列，如果不存在执行buffer
		if (BUFFER_QUEUE == null) {
			BUFFER_QUEUE = new ConcurrentHashMap<>();
		}
		ArrayBlockingQueue<Long> ids = null;
		if (!BUFFER_QUEUE.containsKey(fullPathString)) {
			ids = new ArrayBlockingQueue<>(BUFFER_SIZE);
		} else {
			ids = BUFFER_QUEUE.get(fullPathString);
		}

		if (ids != null) {
			if (ids.size() <= 0) {
				bufferIds(scopeName, schemeName, fieldName);
				ids = BUFFER_QUEUE.get(fullPathString);
			}
			result = ids.poll();
			if (ids.size() <= 0) {
				bufferIds(scopeName, schemeName, fieldName);
			}
		}
		return result;
	}

	@Override
	public Boolean ifExistScope(String scopeName, String schemeName,
			String fieldName) {
		String fullPathString = ROOT_NODE + "/" + scopeName + "/" + schemeName
				+ "/" + fieldName;
		try {
			return client.checkExists().forPath(fullPathString) != null;
		} catch (Exception e) {
			logger.error("验证作用域时发生异常。", e);
			return null;
		}
	}

	private void bufferIds(String scopeName, String schemeName, String fieldName) {
		String fullPathString = ROOT_NODE + "/" + scopeName + "/" + schemeName
				+ "/" + fieldName;
		// 1.获取到当前数据及其当前状态信息
		try {
			Stat stat = client.checkExists().forPath(fullPathString);
			if (stat != null) {
				if (BUFFER_QUEUE == null) {
					synchronized (syncRoot) {
						BUFFER_QUEUE = new ConcurrentHashMap<>();
					}
				}
				byte[] data = client.getData().storingStatIn(stat)
						.forPath(fullPathString);

				if (stat != null && data != null) {
					String dataString = new String(data, "UTF-8");
					String[] seqInfo = dataString.split("\\+");
					Long header = Long.valueOf(seqInfo[0]);
					Long initialValue = Long.valueOf(seqInfo[1]);
					Long stepping = Long.valueOf(seqInfo[2]);
					Long newHeader = header + stepping * BUFFER_SIZE;
					synchronized (syncRoot) {
						client.setData()
								.withVersion(stat.getVersion())
								.forPath(
										fullPathString,
										(newHeader + "+" + initialValue + "+" + stepping)
												.getBytes());
						// 2.生成足够多id到队列中，需要先检查是否存在该队列，如果没有则先创建队列
						ArrayBlockingQueue<Long> queue = generateIdQueue(
								new ArrayBlockingQueue<Long>(BUFFER_SIZE),
								header, stepping);
						// 3.成功后更新当前值到zoo并声明版本号，如果失败则需要清空buff重新生成id，循环多次直到确认步进为最新为止
						BUFFER_QUEUE.put(fullPathString, queue);
					}
				}
			}
		} catch (Exception e) {
			logger.error("缓冲全局ID时失败。", e);
			if (BUFFER_QUEUE.containsKey(fullPathString)) {
				BUFFER_QUEUE.remove(fullPathString);
			}
		}
	}

	private void init() {
		client.start();
		if (BUFFER_QUEUE == null) {
			synchronized (syncRoot) {
				BUFFER_QUEUE = new ConcurrentHashMap<>();
			}
		}
	}

	private ArrayBlockingQueue<Long> generateIdQueue(
			ArrayBlockingQueue<Long> queue, Long header, Long stepping) {
		if (header < 0)
			throw new IllegalArgumentException("起始值必须不小于0.");
		if (stepping < 0)
			throw new IllegalArgumentException("步进必须大于0.");
		synchronized (syncRoot) {
			if (queue == null) {
				queue = new ArrayBlockingQueue<Long>(BUFFER_SIZE);
			}
			queue.clear();
			Long offset = 0L;
			for (long index = 0; index < BUFFER_SIZE; index++) {
				offset += stepping;
				try {
					queue.put(header + offset);
				} catch (InterruptedException e) {
					logger.error("生成ID时发生异常。", e);
					queue.clear();
					break;
				}
			}
		}
		return queue;
	}

}
