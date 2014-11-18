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

	private static final String COUNTER_GROUP_NAME = "counterGroupName";
	private static final String COUNTERS = "counters";
	private static final String COUNTER_GROUPS = "counterGroups";
	private static final String DAG_COUNTERS = "org.apache.tez.common.counters.DAGCounter";
	private static final String ENTITY = "entity";
	private static final String FILE_SYSTEM_COUNTER = "org.apache.tez.common.counters.FileSystemCounter";
	private static final String OTHER_INFO = "otherinfo";
	private static final String TEZ_TASK_COUNTER = "org.apache.tez.common.counters.TaskCounter";
	private static final String TIME_TAKEN = "timeTaken";
	
	private static final String INIT_REQUESTED_TIME = "initRequestedTime";
	private static final String INIT_TIME = "initTime";
	
	private static final String PROCESSOR_CLASS_NAME = "processorClassName";
	private static final String START_REQUESTED_TIME = "startRequestedTime";
	private static final String START_TIME = "startTime";
	private static final String END_TIME = "endTime";
	private static final String STATUS = "status";
	private static final String DIAGNOSTICS = "diagnostics";
	private static final String NUM_TASKS = "numTasks";
	private static final String KILLED = "KILLED";
	private static final String DATA_LOCAL_TASKS = "DATA_LOCAL_TASKS";
	private static final String RACK_LOCAL_TASKS = "RACK_LOCAL_TASKS";
	private static final String FILE_BYTES_READ = "FILE_BYTES_READ";
	private static final String FILE_BYTES_WRITTEN = "FILE_BYTES_WRITTEN";
	private static final String FILE_READ_OPS = "FILE_READ_OPS";
	private static final String FILE_LARGE_READ_OPS = "FILE_LARGE_READ_OPS";
	private static final String FILE_WRITE_OPS = "FILE_WRITE_OPS";
	private static final String HDFS_BYTES_READ= "HDFS_BYTES_READ";
	private static final String HDFS_BYTES_WRITTEN= "HDFS_BYTES_WRITTEN";
	private static final String HDFS_READ_OPS= "HDFS_READ_OPS";
	private static final String HDFS_LARGE_READ_OPS= "HDFS_LARGE_READ_OPS";
	private static final String HDFS_WRITE_OPS= "HDFS_WRITE_OPS";
	private static final String GC_TIME_MILLIS= "GC_TIME_MILLIS";
	private static final String CPU_MILLISECONDS= "CPU_MILLISECONDS";
	private static final String PHYSICAL_MEMORY_BYTES= "PHYSICAL_MEMORY_BYTES";
	private static final String VIRTUAL_MEMORY_BYTES= "VIRTUAL_MEMORY_BYTES";
	private static final String COMMITTED_HEAP_BYTES = "COMMITTED_HEAP_BYTES";
	private static final String VERTEX_NAME = "vertexName";
	private static final String PROCESSOR_CLASS = "processorClass";
	private static final String OUT_EDGE_IDS = "outEdgeIds";
	private static final String IN_EDGE_IDS = "inEdgeIds";
	private static final String ADDITIONAL_OUTPUTS = "additionalOutputs";
	private static final String ADDITIONAL_INTPUTS = "additionalInputs";
	private static final String JSON_OBJECT = "org.json.JSONObject";

	public static String getVertexSummaryHeader(List<String> aggregatedInfoKeys) {
		String header = "ParentDagId,VertexName,Inputs,Destination,DataMovementType,AdditionalInputs,AdditionalOutputs,InitRequestedTime,InitTime,ProcessorClassName,StartRequestedTime,StartTime,EndTime,TimeTaken,Status,NumTasks,DataLocalTask,RackLocalTasks,FileBytesRead,FileBytesWritten,FileReadOps,FileLargeReadOps,FileWriteOps,HDFSBytesRead,HDFSBytesWritten,HDFSReadOps,HDFSLargeReadOps,HDFSWriteOps,GcTimeMs,CpuMs,PhysicalMemoryBytes,VirtualMemoryBytes,CommittedHeapBytes";

		if (aggregatedInfoKeys.size() > 0)
		{
			for (String key : aggregatedInfoKeys) {
				header += "," + key;
			}
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

	long vertexTimeTaken;

	public Vertex(JSONObject jsonObject) {

		this.vertexName = jsonObject.getString(VERTEX_NAME);
		this.vertexProcessorClass = jsonObject.getString(PROCESSOR_CLASS);
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

		if (jsonObject.has(OUT_EDGE_IDS)) {
			vertexOutEdgeId = jsonObject.get(OUT_EDGE_IDS).toString()
					.replace("[", "").replace("]", "").replace("\"", "");
		} else {
			this.vertexOutEdgeId = "";
		}

		if (jsonObject.has(IN_EDGE_IDS)) {
			String bla = jsonObject.get(IN_EDGE_IDS).toString();
			vertexInEdgeIds = Arrays.asList(bla.replace("[", "")
					.replace("]", "").replace("\"", "").split(","));
		}

		if (jsonObject.has(ADDITIONAL_OUTPUTS)) {
			String objectType = jsonObject.get(ADDITIONAL_OUTPUTS).getClass()
					.getName();

			if (objectType.equals(JSON_OBJECT)) {
				this.vertexAdditionalOutputs = new AdditionalOutputs(
						jsonObject.getJSONObject(ADDITIONAL_OUTPUTS));
			} else {
				this.vertexAdditionalOutputs = new AdditionalOutputs(jsonObject
						.getJSONArray(ADDITIONAL_OUTPUTS).getJSONObject(0));
			}
		}

		if (jsonObject.has(ADDITIONAL_INTPUTS)) {
			String objectType = jsonObject.get(ADDITIONAL_INTPUTS).getClass()
					.getName();
			if (objectType.equals(JSON_OBJECT)) {
				this.vertexAdditionalInputs = new AdditionalInputs(
						jsonObject.getJSONObject(ADDITIONAL_INTPUTS));
			} else {
				this.vertexAdditionalInputs = new AdditionalInputs(jsonObject
						.getJSONArray(ADDITIONAL_INTPUTS).getJSONObject(0));
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

		String parentDagEntity=null;
		if (vertexParentDag != null)
		{
			parentDagEntity = vertexParentDag.getEntity();
		}
		
		String values = parentDagEntity + "," + vertexName + ","
				+ vertexInputs + "," + vertexOutName + "," + dataMovementType
				+ "," + vertexAdditionalInputName + ","
				+ vertexAdditionalOutputName + ","
				+ vertexCounters.get(INIT_REQUESTED_TIME) + ","
				+ vertexCounters.get(INIT_TIME) + ","
				+ vertexCounters.get(PROCESSOR_CLASS_NAME) + ","
				+ vertexCounters.get(START_REQUESTED_TIME) + ","
				+ vertexCounters.get(START_TIME) + "," + vertexEndTime + ","
				+ vertexTimeTaken + "," + vertexStatus + ","
				+ vertexCounters.get(NUM_TASKS) + ","
				+ dagCountersHashMap.get(DATA_LOCAL_TASKS) + ","
				+ dagCountersHashMap.get(RACK_LOCAL_TASKS) + ","
				+ fileSystemCountersHashMap.get(FILE_BYTES_READ) + ","
				+ fileSystemCountersHashMap.get(FILE_BYTES_WRITTEN) + ","
				+ fileSystemCountersHashMap.get(FILE_READ_OPS) + ","
				+ fileSystemCountersHashMap.get(FILE_LARGE_READ_OPS) + ","
				+ fileSystemCountersHashMap.get(FILE_WRITE_OPS) + ","
				+ fileSystemCountersHashMap.get(HDFS_BYTES_READ) + ","
				+ fileSystemCountersHashMap.get(HDFS_BYTES_WRITTEN) + ","
				+ fileSystemCountersHashMap.get(HDFS_READ_OPS) + ","
				+ fileSystemCountersHashMap.get(HDFS_LARGE_READ_OPS) + ","
				+ fileSystemCountersHashMap.get(HDFS_WRITE_OPS) + ","
				+ taskCountersHashMap.get(GC_TIME_MILLIS) + ","
				+ taskCountersHashMap.get(CPU_MILLISECONDS) + ","
				+ taskCountersHashMap.get(PHYSICAL_MEMORY_BYTES) + ","
				+ taskCountersHashMap.get(VIRTUAL_MEMORY_BYTES) + ","
				+ taskCountersHashMap.get(COMMITTED_HEAP_BYTES);

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
	
	public void AdjustTimings ()
	{
		long minVertexnStartTime = Long.MAX_VALUE;
		long maxVertexStartTime = Long.MIN_VALUE;
		
		for (Task task : vertexTasks) {
			long taskStartTime = task.taskStartTime;
			long taskEndTime = task.taskEndTime;
			
			if (taskStartTime < minVertexnStartTime)
				minVertexnStartTime = taskStartTime;
			
			if (taskEndTime > maxVertexStartTime)
				maxVertexStartTime = taskEndTime;
		}
		
		vertexEndTime = maxVertexStartTime;
		vertexCounters.get(START_TIME);
		vertexCounters.put(START_TIME, Long.toString(minVertexnStartTime));
		vertexTimeTaken = maxVertexStartTime - minVertexnStartTime;
		
	}

	public void HandleFinishedEvent(JSONObject jsonObject) {
		JSONObject otherInfoJson = jsonObject.getJSONObject(OTHER_INFO);

		try
		{
		vertexTimeTaken = (long) otherInfoJson.get(TIME_TAKEN);
		}
		catch (ClassCastException e)
		{
			vertexTimeTaken = -1;
		}
		vertexEndTime = otherInfoJson.getLong(END_TIME);
		vertexStatus = otherInfoJson.getString(STATUS);
		vertexDiagnostics = otherInfoJson.getString(DIAGNOSTICS);

		if (vertexStatus.equalsIgnoreCase(KILLED)) {
			return;
		}

		JSONArray ja = new JSONArray();
		if (otherInfoJson.getJSONObject(COUNTERS).has(COUNTER_GROUPS))
		{
			ja = otherInfoJson.getJSONObject(COUNTERS).getJSONArray(COUNTER_GROUPS);	
		}

		for (int i = 0; i < ja.length(); i++) {
			JSONObject currentCountersSet = ja.getJSONObject(i);
			String currentGroupName = currentCountersSet
					.getString(COUNTER_GROUP_NAME);

			String objectType = currentCountersSet.get(COUNTERS).getClass()
					.getName();
			JSONArray countersArray = null;
			JSONObject countersObject = null;

			if (objectType.equals(JSON_OBJECT)) {
				countersObject = currentCountersSet.getJSONObject(COUNTERS);
			} else {
				countersArray = currentCountersSet.getJSONArray(COUNTERS);
			}

			switch (currentGroupName) {

			// Parse dag counters such as TOTAL_LAUNCHED_TASKS, DATA_LOCAL_TASKS
			// and RACK_LOCAL_TASKS
			case DAG_COUNTERS: {
				Utils.parseKeyValuePairs(countersArray, countersObject,
						dagCountersHashMap);
			}
				;
				break;

			// Parse file systems counters such as FILE_BYTES_READ,
			// FILE_BYTES_WRITTEN, FILE_READ_OPS,HDFS_BYTES_READ etc..
			case FILE_SYSTEM_COUNTER: {
				Utils.parseKeyValuePairs(countersArray, countersObject,
						fileSystemCountersHashMap);
			}
				;
				break;

			// Parse file systems counters such as GC_TIME_MILLIS,
			// CPU_MILLISECONDS, PHYSICAL_MEMORY_BYTES,VIRTUAL_MEMORY_BYTES
			// etc..
			case TEZ_TASK_COUNTER: {
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

		//Use the Min start time and max start time from the tasks for the vertex start and end time
		AdjustTimings();
		
		vertexParsingComplete = true;
	}

	public void HandleInitializedEvent(JSONObject jsonObject, Dag parentDag) {
		this.vertexEntity = jsonObject.get(ENTITY).toString();
		this.vertexParentDag = parentDag;
		JSONObject otherInfoJSON = jsonObject.getJSONObject(OTHER_INFO);
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
