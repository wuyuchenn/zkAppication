package com.wyc.zkAppication;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ZKClientTest {

	public static void main(String[] args) throws Exception {

		ZkClient zkClient = new ZkClient("localhost:2182");

		zkClient.create("/test/server", "server1", CreateMode.EPHEMERAL_SEQUENTIAL);

		String aa = zkClient.readData("/dshu", true);
		System.out.println(aa);

		zkClient.subscribeChildChanges("/test", new IZkChildListener() {
			@Override
			public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {

			}
		});

		zkClient.subscribeDataChanges("/assingment", new IZkDataListener() {
			@Override
			public void handleDataChange(String dataPath, Object data) throws Exception {
				System.out.println("Data of " + dataPath + " has changed. new data：" + data);
			}

			@Override
			public void handleDataDeleted(String dataPath) throws Exception {
				System.out.println(dataPath + " has deleted");
			}
		});

		ZkClient zkClient4subStat = new ZkClient("localhost:2182");
		zkClient4subStat.subscribeStateChanges(new IZkStateListener() {
			@Override
			public void handleNewSession() throws Exception {
				System.out.println("handleNewSession()");
			}

			@Override
			public void handleStateChanged(KeeperState stat) throws Exception {
				System.out.println("handleStateChanged,stat:" + stat);
			}

			@Override
			public void handleSessionEstablishmentError(Throwable error) throws Exception {

			}
		});

		Thread.sleep(Integer.MAX_VALUE);

	}
}
