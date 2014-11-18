import java.util.HashMap;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

public class Task {

	private static final String COUNTER_GROUP_NAME = "counterGroupName";
	private static final String COUNTER_NAME = "counterName";
	private static final String COUNTERS = "counters";
	private static final String COUNTER_VALUE = "counterValue";
	private static final String DAG_COUNTERS = "org.apache.tez.common.counters.DAGCounter";
	private static final String FILE_SYSTEM_COUNTER = "org.apache.tez.common.counters.FileSystemCounter";
	private static final String TASK_COUNTER = "org.apache.tez.common.counters.TaskCounter";

	HashMap<String, Long> aggregatedInfo;
	HashMap<String, String> dagCountersHashMap;
	HashMap<String, String> fileSystemCountersHashMap;

	String taskContainerId;
	HashMap<String, String> taskCounters;
	HashMap<String, String> taskCountersHashMap;
	String taskDiagnostics;
	Long taskEndTime;
	String taskId;
	String taskNodeName;
	boolean taskParsingComplete;
	Vertex taskPartentVertex;
	Long taskStartTime;

	String taskStatus;
	int taskTimeTaken;
	HashMap<String, HashMap<String, String>> vertexCountersHashMap;

	public Task(Vertex parentVertex, JSONObject jsonObject) {

		taskParsingComplete = false;
		this.taskId = jsonObject.get("entity").toString();
		this.taskPartentVertex = parentVertex;
		this.taskCounters = new HashMap<>();
		this.dagCountersHashMap = new HashMap<>();
		this.fileSystemCountersHashMap = new HashMap<>();
		this.taskCountersHashMap = new HashMap<>();
		this.vertexCountersHashMap = new HashMap<String, HashMap<String, String>>();
		this.aggregatedInfo = new HashMap<String, Long>();

		JSONObject otherInfoJSON = jsonObject.getJSONObject("otherinfo");
		for (String key : otherInfoJSON.keySet()) {
			taskCounters.put(key, otherInfoJSON.get(key).toString());
		}
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return super.equals(obj);
	}

	public String getTaskId() {
		return taskId;
	}

	public static String getTaskSummaryHeader(List<String> aggregatedInfoKeys) {
		String header = "ParentDagId,VertexName,TaskNodeName,TaskContainerId,TaskId,TaskTimeTaken,ScheduledTime,StartTime,TaskEndTime,Status,"
				+ " DataLocalTask,RackLocalTasks,FileBytesRead,FileBytesWritten,FileReadOps,FileLargeReadOps,FileWriteOps,HDFSBytesRead,HDFSBytesWritten,HDFSReadOps,HDFSLargeReadOps,HDFSWriteOps,GcTimeMs,CpuMs,PhysicalMemoryBytes,VirtualMemoryBytes,CommittedHeapBytes";

		for (String key : aggregatedInfoKeys) {
			header += "," + key;
		}

		return header;
	}

	public String getTaskValues(List<String> aggregatedInfoKeys) {
		String values = taskPartentVertex.getParentDagId() + ","
				+ taskPartentVertex.getVertexName() + "," + taskNodeName + ","
				+ taskContainerId + "," + taskId + "," + taskTimeTaken + ","
				+ taskCounters.get("scheduledTime") + "," + taskStartTime + ","
				+ taskEndTime + "," + taskStatus + ","
				+ dagCountersHashMap.get("DATA_LOCAL_TASKS") + ","
				+ dagCountersHashMap.get("RACK_LOCAL_TASKS") + ","
				+ fileSystemCountersHashMap.get("FILE_BYTES_READ") + ","
				+ fileSystemCountersHashMap.get("FILE_BYTES_WRITTEN") + ","
				+ fileSystemCountersHashMap.get("FILE_READ_OPS") + ","
				+ fileSystemCountersHashMap.get("FILE_LARGE_READ_OPS") + ","
				+ fileSystemCountersHashMap.get("FILE_WRITE_OPS") + ","
				+ fileSystemCountersHashMap.get("HDFS_BYTES_READ") + ","
				+ fileSystemCountersHashMap.get("HDFS_BYTES_WRITTEN") + ","
				+ fileSystemCountersHashMap.get("HDFS_READ_OPS") + ","
				+ fileSystemCountersHashMap.get("HDFS_LARGE_READ_OPS") + ","
				+ fileSystemCountersHashMap.get("HDFS_WRITE_OPS") + ","
				+ taskCountersHashMap.get("GC_TIME_MILLIS") + ","
				+ taskCountersHashMap.get("CPU_MILLISECONDS") + ","
				+ taskCountersHashMap.get("PHYSICAL_MEMORY_BYTES") + ","
				+ taskCountersHashMap.get("VIRTUAL_MEMORY_BYTES") + ","
				+ taskCountersHashMap.get("COMMITTED_HEAP_BYTES");

		for (String key : aggregatedInfoKeys) {
			if (aggregatedInfo.containsKey(key)) {
				values += "," + aggregatedInfo.get(key).toString();
			} else {
				values += ",";
			}
		}

		return values;
	}

	public void HandleAttemptEvent(JSONObject jo) {
		this.taskNodeName = jo.getJSONArray("relatedEntities").getJSONObject(0)
				.get("entity").toString();
		this.taskContainerId = jo.getJSONArray("relatedEntities")
				.getJSONObject(1).get("entity").toString();
	}

	public void HandleFinishedEvent(JSONObject jsonObject) {
		JSONObject otherInfoJson = jsonObject.getJSONObject("otherinfo");

		taskTimeTaken = (int) otherInfoJson.get("timeTaken");
		taskStartTime = otherInfoJson.getLong("startTime");
		taskEndTime = otherInfoJson.getLong("endTime");
		taskStatus = otherInfoJson.getString("status");
		taskDiagnostics = otherInfoJson.getString("diagnostics");

		if (taskStatus.equalsIgnoreCase("KILLED")) {
			return;
		}

		JSONArray ja = new JSONArray();
		if (otherInfoJson.getJSONObject("counters").has("counterGroups")) {
			ja = otherInfoJson.getJSONObject("counters").getJSONArray(
					"counterGroups");
		}

		for (int i = 0; i < ja.length(); i++) {
			JSONObject currentCountersSet = ja.getJSONObject(i);
			String currentGroupName = currentCountersSet
					.getString(COUNTER_GROUP_NAME);

			String objectType = currentCountersSet.get("counters").getClass()
					.getName();
			JSONArray countersArray = null;
			JSONObject countersObject = null;
			if (objectType.equals("org.json.JSONObject")) {
				countersObject = currentCountersSet.getJSONObject(COUNTERS);
			} else {
				countersArray = currentCountersSet.getJSONArray(COUNTERS);
			}

			switch (currentGroupName) {

			// Parse dag counters such as TOTAL_LAUNCHED_TASKS, DATA_LOCAL_TASKS
			// and RACK_LOCAL_TASKS
			case DAG_COUNTERS: {
				if (countersArray == null) {
					parseKeyValuePairs(countersObject, dagCountersHashMap);
				} else {
					parseKeyValuePairs(countersArray, dagCountersHashMap);
				}

			}
			;
			break;

			// Parse file systems counters such as FILE_BYTES_READ,
			// FILE_BYTES_WRITTEN, FILE_READ_OPS,HDFS_BYTES_READ etc..
			case FILE_SYSTEM_COUNTER: {
				parseKeyValuePairs(countersArray, fileSystemCountersHashMap);
			}
			;
			break;

			// Parse file systems counters such as GC_TIME_MILLIS,
			// CPU_MILLISECONDS, PHYSICAL_MEMORY_BYTES,VIRTUAL_MEMORY_BYTES
			// etc..
			case TASK_COUNTER: {
				parseKeyValuePairs(countersArray, taskCountersHashMap);
			}
			;
			break;

			default: {
				HashMap<String, String> taskCountersMap = new HashMap<String, String>();
				if (countersArray == null) {
					parseKeyValuePairs(countersObject, taskCountersMap);
				} else {
					parseKeyValuePairs(countersArray, taskCountersMap);
				}
				vertexCountersHashMap.put(currentGroupName, taskCountersMap);
			}
			break;
			}
		}

		aggregatedInfo = Utils.AggregateTaskCounters(vertexCountersHashMap);

		taskParsingComplete = true;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}

	public void parseKeyValuePairs(JSONArray ja, HashMap<String, String> hashmap) {
		for (int j = 0; j < ja.length(); j++) {
			hashmap.put(ja.getJSONObject(j).getString(COUNTER_NAME), ja
					.getJSONObject(j).get(COUNTER_VALUE).toString());
		}
	}

	public void parseKeyValuePairs(JSONObject jo,
			HashMap<String, String> hashmap) {
		hashmap.put(jo.getString(COUNTER_NAME), jo.get(COUNTER_VALUE)
				.toString());
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

}
