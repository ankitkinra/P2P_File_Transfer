package org.umn.distributed.p2p.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.umn.distributed.p2p.common.Machine;
import org.umn.distributed.p2p.common.Utils;

public class TrackingServer {
	public abstract class AbstractServer implements TcpServerDelegate {

		protected Logger logger = Logger.getLogger(this.getClass());
		private TCPServer tcpServer;
		protected int port;
		protected Machine myInfo;
		protected HashMap<String, HashSet<Machine>> filesServersMap = new HashMap<String, HashSet<Machine>>();
		private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
		protected final Lock readL = rwl.readLock();
		protected final Lock writeL = rwl.writeLock();

		protected AbstractServer(int port, int numTreads) {
			this.port = port;
			this.tcpServer = new TCPServer(this, numTreads);
		}

		public void start() throws Exception {
			logger.info("****************Starting Server****************");
			try {
				this.port = this.tcpServer.startListening(this.port);
				myInfo = new Machine(Utils.getLocalServerIp(), this.port);
				startSpecific();
			} catch (IOException ioe) {
				logger.error("Error starting tcp server. Stopping now", ioe);
				this.stop();
				throw ioe;
			}
		}

		public abstract void startSpecific() throws Exception;

		public abstract void showInfo();

		protected boolean addFile(String fileName, Machine machine) {
			writeL.lock();
			try {
				HashSet<Machine> machinesForArticle = null;
				if (this.filesServersMap.containsKey(fileName)) {
					machinesForArticle = this.filesServersMap.get(fileName);
				} else {
					machinesForArticle = new HashSet<Machine>();
				}
				machinesForArticle.add(machine);
				this.filesServersMap.put(fileName, machinesForArticle);
				return true;// TODO
			} finally {
				writeL.unlock();
			}
		}

		/**
		 * TODO Hard problem as this machine is scattered in all of the
		 * articles, so we need to look at every article which is stored
		 * 
		 * @param id
		 * @return
		 */
		protected void removeMachine(Machine m) {
			writeL.lock();
			try {
				for (Entry<String, HashSet<Machine>> entry : this.filesServersMap
						.entrySet()) {
					entry.getValue().remove(m); // for each article remove the
												// peer
				}
			} finally {
				writeL.unlock();
			}
		}

		protected Set<Machine> getMachinesForArticle(String article) {
			Set<Machine> machineSet = new HashSet<Machine>();
			readL.lock();
			try {
				if (this.filesServersMap.containsKey(article)) {
					machineSet.addAll(this.filesServersMap.get(article));
				} else {
					logger.warn("tried to find machines for an unknown article ="
							+ article);
				}
			} finally {
				readL.unlock();
			}
			return machineSet;
		}

		public int getInternalPort() {
			return this.port;
		}

		public void stop() {
			this.tcpServer.stop();
		}
	}
}