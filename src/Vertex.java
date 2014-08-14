import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

public class Vertex {

	class AdditionalInputs {
		String className;
		String initializer;
		String name;

		public AdditionalInputs(JSONObject jo) {
			this.className = jo.getString("class");
			if (jo.has("initializer")) {
				this.initializer = jo.getString("initializer");
			} else {
				this.initializer = "";
			}

			this.name = jo.getString("name");
		}

		public String getClassName() {
			return className;
		}

		public String getInitializer() {
			return initializer;
		}

		public String getName() {
			return name;
		}

		public void setClassName(String className) {
			this.className = className;
		}

		public void setInitializer(String initializer) {
			this.initializer = initializer;
		}

		@Override
		public String toString() {
			return "AdditionalInputs [className=" + className
					+ ", initializer=" + initializer + ", name=" + name + "]";
		}
	}

	private class AdditionalOutputs {

		String className;
		String name;

		public AdditionalOutputs(JSONObject jo) {
			this.className = jo.getString("class");
			this.name = jo.getString("name");
		}

		@Override
		public boolean equals(Object obj) {
			// TODO Auto-generated method stub
			return super.equals(obj);
		}

		public String getName() {
			return name;
		}

		@Override
		public String toString() {
			return "AdditionalOutputs [className=" + className + ", name="
					+ name + "]";
		}

	}

	public enum EVENT_TYPES {
		VERTEX_FINISHED("VERTEX_FINISHED"), VERTEX_INITIALIZED(
				"VERTEX_INITIALIZED"), VERTEX_STARTED("VERTEX_STARTED");

		private final String text;

		/**
		 * @param text
		 */
		private EVENT_TYPES(final String text) {
			this.text = text;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Enum#toString()
		 */
		@Override
		public final String toString() {
			return text;
		}
	}

	private static final String counterGroupName = "counterGroupName";
	private static final String counters = "counters";
	private static final String dagCounters = "org.apache.tez.common.counters.DAGCounter";
	private static final String entity = "entity";
	private static final String fileSystemCounter = "org.apache.tez.common.counters.FileSystemCounter";
	private static final String otherinfo = "otherinfo";
	private static final String taskCounter = "org.apache.tez.common.counters.TaskCounter";

	public static String getVertexSummaryHeader(List<String> aggregatedInfoKeys) {
		String header = "ParentDagId,VertexName,Inputs,Destination,DataMovementType,AdditionalInputs,AdditionalOutputs,InitRequestedTime,InitTime,ProcessorClassName,StartRequestedTime,StartTime,EndTime,TimeTaken,Status,NumTasks,DataLocalTask,RackLocalTasks,FileBytesRead,FileBytesWritten,FileReadOps,FileLargeReadOps,FileWriteOps,HDFSBytesRead,HDFSBytesWritten,HDFSReadOps,HDFSLargeReadOps,HDFSWriteOps,GcTimeMs,CpuMs,PhysicalMemoryBytes,VirtualMemoryBytes,CommittedHeapBytes";

		for (String key : aggregatedInfoKeys) {
			header += "," + key;
		}

		return header;
	}
	HashMap<String, Long> aggregatedInfo;

	HashMap<String, String> dagCountersHashMap;
	HashMap<String, String> fileSystemCountersHashMap;

	HashMap<String, String> taskCountersHashMap;
	AdditionalInputs vertexAdditionalInputs;
	AdditionalOutputs vertexAdditionalOutputs;
	HashMap<String, String> vertexCounters;
	HashMap<String, HashMap<String, String>> vertexCountersHashMap;
	String vertexDiagnostics;
	long vertexEndTime;

	String vertexEntity;
	List<String> vertexInEdgeIds;
	List<Edge> vertexInputEdges;
	String vertexName;
	Edge vertexOutEdge;
	String vertexOutEdgeId;
	Dag vertexParentDag;
	boolean vertexParsingComplete;
	String vertexProcessorClass;
	String vertexStatus;
	List<Task> vertexTasks;

	int vertexTimeTaken;

	public Vertex(JSONObject jsonObject) {

		this.vertexName = jsonObject.getString("vertexName");
		this.vertexProcessorClass = jsonObject.getString("processorClass");
		this.vertexCounters = new HashMap<>();

		this.dagCountersHashMap = new HashMap<String, String>();
		this.fileSystemCountersHashMap = new HashMap<String, String>();
		this.taskCountersHashMap = new HashMap<String, String>();
		this.vertexCountersHashMap = new HashMap<String, HashMap<String, String>>();
		this.aggregatedInfo = new HashMap<String, Long>();
		this.vertexParsingComplete = false;
		this.vertexInputEdges = new ArrayList<>();
		this.vertexInEdgeIds = new ArrayList<>();
		this.vertexTasks = new ArrayList<>();

		if (jsonObject.has("outEdgeIds")) {
			vertexOutEdgeId = jsonObject.get("outEdgeIds").toString()
					.replace("[", "").replace("]", "").replace("\"", "");
		} else {
			this.vertexOutEdgeId = "";
		}

		if (jsonObject.has("inEdgeIds")) {
			String bla = jsonObject.get("inEdgeIds").toString();
			vertexInEdgeIds = Arrays.asList(bla.replace("[", "")
					.replace("]", "").replace("\"", "").split(","));
		}

		if (jsonObject.has("additionalOutputs")) {
			String objectType = jsonObject.get("additionalOutputs").getClass()
					.getName();

			if (objectType.equals("org.json.JSONObject")) {
				this.vertexAdditionalOutputs = new AdditionalOutputs(
						jsonObject.getJSONObject("additionalOutputs"));
			} else {
				this.vertexAdditionalOutputs = new AdditionalOutputs(jsonObject
						.getJSONArray("additionalOutputs").getJSONObject(0));
			}
		}

		if (jsonObject.has("additionalInputs")) {
			String objectType = jsonObject.get("additionalInputs").getClass()
					.getName();
			if (objectType.equals("org.json.JSONObject")) {
				this.vertexAdditionalInputs = new AdditionalInputs(
						jsonObject.getJSONObject("additionalInputs"));
			} else {
				this.vertexAdditionalInputs = new AdditionalInputs(jsonObject
						.getJSONArray("additionalInputs").getJSONObject(0));
			}

		}
	}

	public void AddEdgetoInputList(Edge edgeToAdd) {
		vertexInputEdges.add(edgeToAdd);
	}

	public void AddTasktoTaskList(Task taskToAdd) {
		vertexTasks.add(taskToAdd);
	}

	public List<String> getInEdgeIds() {
		return vertexInEdgeIds;
	}

	public AdditionalInputs getInputs() {
		return vertexAdditionalInputs;
	}

	public String getOutEdgeId() {
		return vertexOutEdgeId;
	}

	public String getParentDagId() {
		return vertexParentDag.getEntity();
	}

	public List<Task> GetTaskList() {
		return vertexTasks;
	}

	public String getVertexEntity() {
		return vertexEntity;
	}

	public String getVertexName() {
		return vertexName;
	}

	public String getVertexValues(List<String> aggregatedInfoKeys) {
		String vertexAdditionalInputName = (vertexAdditionalInputs == null) ? null
				: vertexAdditionalInputs.getName();
		String vertexAdditionalOutputName = (vertexAdditionalOutputs == null) ? null
				: vertexAdditionalOutputs.getName();
		String vertexOutName = (vertexOutEdge == null) ? null : vertexOutEdge
				.getOutputVertexName();
		String dataMovementType = (vertexOutEdge == null) ? null
				: vertexOutEdge.getDataMovementType();
		String vertexInputs = "";

		if (vertexInputEdges.size() > 0) {
			for (Edge currentEdge : vertexInputEdges) {
				vertexInputs += (currentEdge == null) ? null : currentEdge
						.getInputVertexName() + " ";
			}
		}

		String values = vertexParentDag.getEntity() + "," + vertexName + ","
				+ vertexInputs + "," + vertexOutName + "," + dataMovementType
				+ "," + vertexAdditionalInputName + ","
				+ vertexAdditionalOutputName + ","
				+ vertexCounters.get("initRequestedTime") + ","
				+ vertexCounters.get("initTime") + ","
				+ vertexCounters.get("processorClassName") + ","
				+ vertexCounters.get("startRequestedTime") + ","
				+ vertexCounters.get("startTime") + "," + vertexEndTime + ","
				+ vertexTimeTaken + "," + vertexStatus + ","
				+ vertexCounters.get("numTasks") + ","
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

		// This doesn't work due to inconsistency in the log lines printed
		/*
		 * for ( String value : dagCountersHashMap.values() ) { values +="," +
		 * value; }
		 * 
		 * for ( String value : fileSystemCountersHashMap.values() ) { values
		 * +="," + value ; }
		 * 
		 * for ( String value : taskCountersHashMap.values() ) { values +="," +
		 * value ; }
		 */

		for (String key : aggregatedInfoKeys) {
			if (aggregatedInfo.containsKey(key)) {
				values += "," + aggregatedInfo.get(key).toString();
			} else {
				values += ",";
			}
		}

		return values;
	}

	public void HandleFinishedEvent(JSONObject jsonObject) {
		JSONObject otherInfoJson = jsonObject.getJSONObject(otherinfo);

		vertexTimeTaken = (int) otherInfoJson.get("timeTaken");
		vertexEndTime = otherInfoJson.getLong("endTime");
		vertexStatus = otherInfoJson.getString("status");
		vertexDiagnostics = otherInfoJson.getString("diagnostics");

		if (vertexStatus.equalsIgnoreCase("KILLED")) {
			return;
		}

		JSONArray ja = otherInfoJson.getJSONObject("counters").getJSONArray(
				"counterGroups");

		for (int i = 0; i < ja.length(); i++) {
			JSONObject currentCountersSet = ja.getJSONObject(i);
			String currentGroupName = currentCountersSet
					.getString(counterGroupName);

			String objectType = currentCountersSet.get("counters").getClass()
					.getName();
			JSONArray countersArray = null;
			JSONObject countersObject = null;

			if (objectType.equals("org.json.JSONObject")) {
				countersObject = currentCountersSet.getJSONObject(counters);
			} else {
				countersArray = currentCountersSet.getJSONArray(counters);
			}

			switch (currentGroupName) {

			// Parse dag counters such as TOTAL_LAUNCHED_TASKS, DATA_LOCAL_TASKS
			// and RACK_LOCAL_TASKS
			case dagCounters: {
				Utils.parseKeyValuePairs(countersArray, countersObject,
						dagCountersHashMap);
			}
				;
				break;

			// Parse file systems counters such as FILE_BYTES_READ,
			// FILE_BYTES_WRITTEN, FILE_READ_OPS,HDFS_BYTES_READ etc..
			case fileSystemCounter: {
				Utils.parseKeyValuePairs(countersArray, countersObject,
						fileSystemCountersHashMap);
			}
				;
				break;

			// Parse file systems counters such as GC_TIME_MILLIS,
			// CPU_MILLISECONDS, PHYSICAL_MEMORY_BYTES,VIRTUAL_MEMORY_BYTES
			// etc..
			case taskCounter: {
				Utils.parseKeyValuePairs(countersArray, countersObject,
						taskCountersHashMap);
			}
				;
				break;

			default: {
				HashMap<String, String> taskCountersMap = new HashMap<String, String>();
				Utils.parseKeyValuePairs(countersArray, countersObject,
						taskCountersMap);
				vertexCountersHashMap.put(currentGroupName, taskCountersMap);
			}
				break;
			}
		}

		aggregatedInfo = Utils.AggregateTaskCounters(vertexCountersHashMap);

		vertexParsingComplete = true;
	}

	public void HandleInitializedEvent(JSONObject jsonObject, Dag parentDag) {
		this.vertexEntity = jsonObject.get(entity).toString();
		this.vertexParentDag = parentDag;
		JSONObject otherInfoJSON = jsonObject.getJSONObject(otherinfo);
		for (String key : otherInfoJSON.keySet()) {
			vertexCounters.put(key, otherInfoJSON.get(key).toString());
		}
	}

	public void HandleStartedEvent(JSONObject jsonObject, Dag parentDag) {
		// For now there is only the otherinfo node so use as is
		this.HandleInitializedEvent(jsonObject, parentDag);
	}

	public void SetVertexOutEdge(Edge edgeToAdd) {
		this.vertexOutEdge = edgeToAdd;
	}

	@Override
	public String toString() {
		return "Vertex [vertexName=" + vertexName + ", processorClass="
				+ vertexProcessorClass + ", outEdgeIds=" + vertexOutEdgeId
				+ ", inEdgeIds=" + vertexInEdgeIds + ", inputs="
				+ vertexAdditionalInputs + ", outputs="
				+ vertexAdditionalOutputs + "]";
	}
}
