import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONObject;


public class Task {

	public Task(Vertex parentVertex, JSONObject jsonObject) {
		
		taskParsingComplete = false;
		this.taskId = jsonObject.get("entity").toString();
		this.taskPartentVertex = parentVertex;
		this.taskCounters = new HashMap<>(); 
		this.dagCountersHashMap = new HashMap<>();
		this.fileSystemCountersHashMap = new HashMap<>();
		this.taskCountersHashMap = new HashMap<>();
		this.vertexCountersHashMap = new HashMap<String, HashMap<String, String>>();
		
		JSONObject otherInfoJSON = jsonObject.getJSONObject("otherinfo");
		for (String key : otherInfoJSON.keySet()) {
			taskCounters.put(key,otherInfoJSON.get(key).toString());
		}
	}
	
	public void HandleAttemptEvent(JSONObject jo)
	{
		this.taskNodeName = jo.getJSONArray("relatedEntities").getJSONObject(0).get("entity").toString();
		this.taskContainerId = jo.getJSONArray("relatedEntities").getJSONObject(1).get("entity").toString();
	}
	
	public void parseKeyValuePairs(JSONArray ja, HashMap<String, String> hashmap )
	{
		 for (int j = 0 ; j < ja.length(); j++)
		 {
			 hashmap.put(ja.getJSONObject(j).getString(counterName), 
					 ja.getJSONObject(j).get(counterValue).toString());
		 }
	}
	
	public void parseKeyValuePairs(JSONObject jo, HashMap<String, String> hashmap )
	{
			 hashmap.put(jo.getString(counterName), 
					 jo.get(counterValue).toString());
	}
	
	public void HandleFinishedEvent(JSONObject jsonObject)
	{
		JSONObject otherInfoJson = jsonObject.getJSONObject("otherinfo");
		
		taskTimeTaken 		= (int) otherInfoJson.get("timeTaken");
		taskStartTime		= otherInfoJson.getLong("startTime");
		taskEndTime 		= otherInfoJson.getLong("endTime");
		taskStatus 			= otherInfoJson.getString("status");
		taskDiagnostics  	= otherInfoJson.getString("diagnostics");
		
		if (taskStatus.equalsIgnoreCase("KILLED"))
		{
			return;
		}
		
		JSONArray ja = otherInfoJson.getJSONObject("counters").getJSONArray("counterGroups");
		for (int i = 0 ; i < ja.length() ; i ++)
		{
			JSONObject currentCountersSet = ja.getJSONObject(i);
			String currentGroupName = currentCountersSet.getString(counterGroupName);
			
			String objectType = currentCountersSet.get("counters").getClass().getName();
			JSONArray countersArray = null;
			JSONObject countersObject = null;
			if (objectType.equals("org.json.JSONObject"))
			{
				countersObject = currentCountersSet.getJSONObject(counters);
			}
			else
			{
				countersArray = currentCountersSet.getJSONArray(counters);
			}
			
			switch (currentGroupName) {

				// Parse dag counters such as TOTAL_LAUNCHED_TASKS, DATA_LOCAL_TASKS and RACK_LOCAL_TASKS
				case dagCounters:  
				{
					if (countersArray == null) {
							parseKeyValuePairs(countersObject,dagCountersHashMap);
					}
					else
					{
							parseKeyValuePairs(countersArray,dagCountersHashMap);
					}
					
				};
				break;
	
				// Parse file systems counters such as FILE_BYTES_READ, FILE_BYTES_WRITTEN, FILE_READ_OPS,HDFS_BYTES_READ etc..
				case fileSystemCounter:  
				{
					parseKeyValuePairs(countersArray,fileSystemCountersHashMap);
				};
				break;
	
				// Parse file systems counters such as GC_TIME_MILLIS, CPU_MILLISECONDS, PHYSICAL_MEMORY_BYTES,VIRTUAL_MEMORY_BYTES etc..
				case taskCounter:  
				{
					parseKeyValuePairs(countersArray,taskCountersHashMap);
				};
				break;
				
				default:
				{
					HashMap<String, String> taskCountersMap = new HashMap<String, String>();
					if (countersArray == null) {
						parseKeyValuePairs(countersObject,taskCountersMap);
					}
					else
					{
							parseKeyValuePairs(countersArray,taskCountersMap);
					}
					vertexCountersHashMap.put(currentGroupName, taskCountersMap);
				}
				break;
			}
		}
		taskParsingComplete = true;
	}
	
	
	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	
	public static String getTaskSummaryHeader()
	{
		String header = "ParentDagId,VertexName,TaskNodeName,TaskId,TaskTimeTaken,ScheduledTime,StartTime,TaskEndTime,Status"
				+" DataLocalTask,RackLocalTasks,FileBytesRead,FileBytesWritten,FileReadOps,FileLargeReadOps,FileWriteOps,HDFSBytesRead,HDFSBytesWritten,HDFSReadOps,HDFSLargeReadOps,HDFSWriteOps,GcTimeMs,CpuMs,PhysicalMemoryBytes,VirtualMemoryBytes,CommittedHeapBytes";
		return header;
	}
	
	public String getTaskValues()
	{
		String values = taskPartentVertex.getParentDagId() + "," + taskPartentVertex.getVertexName()  + "," + taskNodeName  + "," +
						taskId + "," + taskTimeTaken + "," + taskCounters.get("scheduledTime")  + "," + taskStartTime + "," + taskEndTime  + "," + 
						taskStatus + "," + dagCountersHashMap.get("DATA_LOCAL_TASKS")  + "," +dagCountersHashMap.get("RACK_LOCAL_TASKS")   + "," +
						fileSystemCountersHashMap.get("FILE_BYTES_READ") + "," + fileSystemCountersHashMap.get("FILE_BYTES_WRITTEN") + "," + fileSystemCountersHashMap.get("FILE_READ_OPS") + "," +
						fileSystemCountersHashMap.get("FILE_LARGE_READ_OPS") + "," + fileSystemCountersHashMap.get("FILE_WRITE_OPS") + "," + fileSystemCountersHashMap.get("HDFS_BYTES_READ") + "," +
						fileSystemCountersHashMap.get("HDFS_BYTES_WRITTEN") + "," + fileSystemCountersHashMap.get("HDFS_READ_OPS") + "," +
						fileSystemCountersHashMap.get("HDFS_LARGE_READ_OPS") + "," + fileSystemCountersHashMap.get("HDFS_WRITE_OPS") + "," +

						taskCountersHashMap.get("GC_TIME_MILLIS") + "," + taskCountersHashMap.get("CPU_MILLISECONDS") + "," + taskCountersHashMap.get("PHYSICAL_MEMORY_BYTES") + "," + 
						taskCountersHashMap.get("VIRTUAL_MEMORY_BYTES") + "," + taskCountersHashMap.get("COMMITTED_HEAP_BYTES");
		
		return values;
	}


	boolean taskParsingComplete;
	int taskTimeTaken;
	Long taskStartTime;
	Long taskEndTime;
	String taskId;
	String taskStatus;
	String taskDiagnostics;
	String taskContainerId;
	Vertex taskPartentVertex;
	String taskNodeName;
	
	HashMap<String, String> taskCounters;
	HashMap<String, String> dagCountersHashMap;
	HashMap<String, String> fileSystemCountersHashMap;
	HashMap<String, String> taskCountersHashMap;
	HashMap<String, HashMap<String, String>> vertexCountersHashMap;
	
	private static final String counterGroupName = "counterGroupName";
	private static final String counterName = "counterName";
	private static final String counters = "counters";
	private static final String counterValue = "counterValue";
	private static final String dagCounters = "org.apache.tez.common.counters.DAGCounter";
	private static final String fileSystemCounter = "org.apache.tez.common.counters.FileSystemCounter";
	private static final String taskCounter = "org.apache.tez.common.counters.TaskCounter";

}