package com.wyc.zkAppication;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;

public class tomcat3 {

	final static String ROOT_PATH = "/zkTest";

	final static String MASTER_PATH = ROOT_PATH + "/master";

	final static String NORMAL_PATH = ROOT_PATH + "/normal";

	final static String ASSIGNMENT_PATH = ROOT_PATH + "/assignment";

	final static String ip = "tomcat3";

	static volatile String currentMaster = "";

	static int dbMinId = 3;
	static int dbMaxId = 26;

	public static void main(String[] args) throws Exception {

		// 模拟项目启动
		ZkClient zkClient = new ZkClient("localhost:2182");

		if (!zkClient.exists(ROOT_PATH)) {
			zkClient.create(ROOT_PATH, "", CreateMode.PERSISTENT); // /zkTest
			System.out.println("create " + ROOT_PATH);
		}
		if (!zkClient.exists(ASSIGNMENT_PATH)) {
			zkClient.create(ASSIGNMENT_PATH, "", CreateMode.PERSISTENT); // /zkTest/assignment
			System.out.println("create " + ASSIGNMENT_PATH);
		}
		if (!zkClient.exists(MASTER_PATH)) {
			zkClient.create(MASTER_PATH, "", CreateMode.PERSISTENT); // /zkTest/master
			System.out.println("create " + MASTER_PATH);
		}
		if (!zkClient.exists(NORMAL_PATH)) {
			zkClient.create(NORMAL_PATH, "", CreateMode.PERSISTENT); // /zkTest/normal
			System.out.println("create " + NORMAL_PATH);
		}

		// 选主
		masterSelection(zkClient, ip);
		if (isMaster(zkClient, ip)) {
			currentMaster = ip;
			System.out.println("i am the maser : " + ip);
		} else {
			currentMaster = getMaster(zkClient);
			System.out.println("existed maser : " + getMaster(zkClient));
		}

		// 注册到normal节点
		zkClient.create(NORMAL_PATH + "/" + ip, ip, CreateMode.EPHEMERAL); // /zkTest/normal/tomcat1 <--> "tomcat1"
		System.out.println("create " + NORMAL_PATH + "/" + ip);

		// 考虑两种异常情况
		// (1)normal节点挂掉,master节点需要reassignment
		zkClient.subscribeChildChanges(NORMAL_PATH, new IZkChildListener() { // 监听/zkTest/noraml节点，normal挂掉的时候去assignment上比对
			@Override
			public void handleChildChange(String parentPath, List<String> normalNodes) throws Exception {
				
				if(!isMaster(zkClient, ip)) {
					return;
				}
				List<String> jobIds = zkClient.getChildren(ASSIGNMENT_PATH);
				if (jobIds != null && jobIds.size() > 0) {
					System.out.println("some node crushed !!!!!");
					for (String jobId : jobIds) { // assignment子节点有的，但normal里没有的，表示当前normal子节点已经挂掉了
						boolean flag = true;
						for (String normalNode : normalNodes) {
							if (jobId != null && jobId != "" && jobId.startsWith(normalNode)) {
								flag = false;
								break;
							}
						}
						if (flag) {
							// 该assignment分配的normal节点已经挂掉了
							System.out.println(ASSIGNMENT_PATH + "/" + jobId + " 等待重新分配");
							String origJob = zkClient.readData(ASSIGNMENT_PATH + "/" + jobId, true);
							System.out.println(ASSIGNMENT_PATH + "/" + jobId + " 原任务值：" + origJob);
							// 重分配
							if (origJob != null && origJob.contains("-")) {
								String[] origJobArr = origJob.split("-");
								assignmentJob(zkClient, Integer.parseInt(origJobArr[1]),
										Integer.parseInt(origJobArr[0]));
								System.out.println(ASSIGNMENT_PATH + "/" + jobId + " 重新分配完成");
								zkClient.delete(ASSIGNMENT_PATH + "/" + jobId);
							}
						}
					}
				}
			}
		});
		// (2)master节点挂掉
		zkClient.subscribeChildChanges(MASTER_PATH, new IZkChildListener() { // 监听/zkTest/master节点，master挂掉的时候要重新选举  
																			// Master挂掉之后，master里它创建的序列节点没了，normal里的它没了，assignment子节点都没有了 
			@Override
			public void handleChildChange(String parentPath, List<String> masterNode) throws Exception {

				String standByMaster = getMaster(zkClient);
				if(!currentMaster.equals(standByMaster) && ip.equals(standByMaster)) {
					System.out.println("master changed , old master : "+ currentMaster);
					currentMaster = standByMaster;
					System.out.println("i am the new master : " + ip);
					// 重新分配任务
					assignmentJob(zkClient, dbMaxId, dbMinId);
				}
				
			}
		});

		zkClient.subscribeChildChanges(ASSIGNMENT_PATH, new IZkChildListener() { // 监听/zkTest/assignment节点( 是用一个线程执行
																					// 所有触发的事件是按顺序执行的 应该是客户端有个队列)
			@Override
			public void handleChildChange(String parentPath, List<String> jobIds) throws Exception {

				if (jobIds != null && jobIds.size() > 0) {
					for (String jobId : jobIds) {
						if (jobId != null && jobId != "" && jobId.startsWith(ip)) {
							String myJob = null;
							while ((myJob = zkClient.readData(ASSIGNMENT_PATH + "/" + jobId, true)) == null) {

							}
							if (myJob != null && myJob != "") {
								// 可以用阻塞队列将任务放进去(具体看业务)
								System.out.println(ip + " job :" + myJob);
								System.out.println(ip + " start job...");
								// 用来模拟服务在执行任务时挂掉的情形
								//Thread.sleep(Integer.MAX_VALUE);
								System.out.println(ip + " finished job");
								// 如果任务完成 该任务节点是会被删除的
								zkClient.delete(ASSIGNMENT_PATH + "/" + jobId);
							}
						}
					}
				}
			}
		});

		// 以上代码均在项目启动后就完成
		Thread.sleep(30000);

		// 分发任务
		assignmentJob(zkClient, dbMaxId, dbMinId);

		Thread.sleep(Integer.MAX_VALUE);
	}

	private static void assignmentJob(ZkClient zkClient, int maxId, int minId) {

		if (zkClient.exists(MASTER_PATH)) {

			if (!isMaster(zkClient, ip)) {
				return;
			}
			// 任务分配
			System.out.println("任务分配数据 ： " + minId + " " + maxId);
			List<String> normalNode = zkClient.getChildren(NORMAL_PATH);
			System.out.println("assignment job , normalNode = " + normalNode);
			int serverCount = normalNode.size();
			String tomcatIp = "";
			
			// 用来模拟master节点在分配任务时挂掉的情景
			/*try {
				Thread.sleep(Integer.MAX_VALUE);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}*/
			
			if ((maxId - minId) <= serverCount) {
				Random random = new Random();
				tomcatIp = normalNode.get(random.nextInt(serverCount));
				String jobId = tomcatIp + "-" + System.currentTimeMillis();
				zkClient.create(ASSIGNMENT_PATH + "/" + jobId, "", CreateMode.EPHEMERAL);
				System.out.println("create " + ASSIGNMENT_PATH + "/" + jobId);
				zkClient.writeData(ASSIGNMENT_PATH + "/" + jobId, minId + "-" + maxId);
				System.out.println("wirte " + ASSIGNMENT_PATH + "/" + jobId + " value : " + minId + "-" + maxId);
			} else {
				int basicNum = (maxId - minId) / serverCount;
				int temp = minId;
				for (int i = 1; i <= serverCount; i++) {
					tomcatIp = normalNode.get(i - 1);
					String jobId = tomcatIp + "-" + System.currentTimeMillis();
					zkClient.create(ASSIGNMENT_PATH + "/" + jobId, "", CreateMode.EPHEMERAL);
					System.out.println("create " + ASSIGNMENT_PATH + "/" + jobId);
					if (i == serverCount) {
						zkClient.writeData(ASSIGNMENT_PATH + "/" + jobId, temp + "-" + maxId);
						System.out.println("wirte " + ASSIGNMENT_PATH + "/" + jobId + " value : " + temp + "-" + maxId);
					} else {
						zkClient.writeData(ASSIGNMENT_PATH + "/" + jobId, temp + "-" + ((minId + i * basicNum) - 1));
						System.out.println("wirte " + ASSIGNMENT_PATH + "/" + jobId + " value : " + temp + "-"
								+ ((minId + i * basicNum) - 1));
					}
					temp = minId + i * basicNum;
				}
			}

		}
	}

	// 选主
	private static void masterSelection(ZkClient zkClient, String ip) {
		
		zkClient.create(MASTER_PATH + "/server", ip, CreateMode.EPHEMERAL_SEQUENTIAL);
	}
	
	// 查看自己是否是Master服务器
	private static boolean isMaster(ZkClient zkClient, String ip) {

		List<String> sequentialNodes = zkClient.getChildren(MASTER_PATH);
		if (sequentialNodes != null && sequentialNodes.size() > 0) {
			Collections.sort(sequentialNodes); 
			String master = zkClient.readData(MASTER_PATH + "/" + sequentialNodes.get(0));
			if (ip.equals(master)) {
				return true;
			}
		}
		return false;
	}

	// 获取Master节点
	private static String getMaster(ZkClient zkClient) {

		List<String> sequentialNodes = zkClient.getChildren(MASTER_PATH);
		if (sequentialNodes != null && sequentialNodes.size() > 0) {
			Collections.sort(sequentialNodes); 
			return zkClient.readData(MASTER_PATH + "/" + sequentialNodes.get(0));
		}
		return "";
	}

}
