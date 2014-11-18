import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

public class Dag {

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) {

		if (this.getClass() != obj.getClass()) {
			return false;
		}

		if (this.dagEntity != ((Dag) obj).dagEntity) {
			return false;
		}

		if (this.dagApplicationId != ((Dag) obj).dagApplicationId) {
			return false;
		}

		if (this.dagStartedTime != ((Dag) obj).dagStartedTime) {
			return false;
		}

		if (this.m_dagName != ((Dag) obj).m_dagName) {
			return false;
		}

		return true;
	}

	public enum EVENT_TYPES {
		DAG_FINISHED("DAG_FINISHED"), DAG_INITIALIZED("DAG_INITIALIZED"), DAG_STARTED(
				"DAG_STARTED"), DAG_SUBMITTED("DAG_SUBMITTED"), TASK_ATTEMPT_STARTED(
						"TASK_ATTEMPT_STARTED"), TASK_FINISHED("TASK_FINISHED"), TASK_STARTED(
								"TASK_STARTED"), VERTEX_FINISHED("VERTEX_FINISHED"), VERTEX_INITIALIZED(
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

	private static final String APPLICATION_ID = "applicationId";
	private static final String COUNTER_GROUP_NAME = "counterGroupName";
	private static final String COUNTERS = "counters";
	private static final String TEZ_DAG_COUNTERS = "org.apache.tez.common.counters.DAGCounter";
	private static final String DAG_PLAN = "dagPlan";
	private static final String VERTEX_ID_MAPPING = "vertexNameIdMapping";
	private static final String DIAGNOSTICS = "diagnostics";
	private static final String EDGES = "edges";
	private static final String END_TIME = "endTime";
	private static final String ENTITIY = "entity";
	private static final String ENTITY_TYPE = "entitytype";
	private static final String EVENTS = "events";
	private static final String EVENT_TYPE = "eventtype";
	private static final String FILE_SYSTEM_COUNTER = "org.apache.tez.common.counters.FileSystemCounter";
	private static final String HIVE_COUNTER = "HIVE";
	private static final String OTHER_INFO = "otherinfo";
	private static final String RELATED_ENTITIES = "relatedEntities";
	private static final String START_TIME = "startTime";
	private static final String STATUS = "status";
	private static final String TASK_COUNTER = "org.apache.tez.common.counters.TaskCounter";
	private static final String TIME_TAKEN = "timeTaken";
	private static final String TS = "ts";
	private static final String VERTEX_NAME = "vertexName";
	private static final String VERTICES = "vertices";

	HashMap<String, Long> aggregatedInfo;
	List<String> aggregatedInfoKeys;
	String dagApplicationId;
	HashMap<String, String> dagCountersHashMap;
	String dagDiagnostics;

	long dagEndTime;
	String dagEntity;
	String dagFinishedTime;
	String dagInitializedTime;
	boolean dagParsingComplete;
	String dagStartedTime;
	long dagStartTime;
	String dagStatus;

	String dagSubmittedTime;
	int dagTimeTaken;
	HashMap<String, Edge> edgesHashMap;
	HashMap<String, String> fileSystemCountersHashMap;
	HashMap<String, String> hiveCountersHashMap;
	HashMap<String, HashMap<String, String>> hiveVertexCountersHashMap;
	String m_dagName;
	HashMap<String, String> taskCountersHashMap;
	HashMap<String, Task> taskIdObjectMap;
	HashMap<String, Vertex> verticesHashMap;

	public Dag() {
		super();
		edgesHashMap = new HashMap<String, Edge>();
		verticesHashMap = new HashMap<String, Vertex>();
		taskIdObjectMap = new HashMap<String, Task>();
		dagCountersHashMap = new HashMap<String, String>();
		fileSystemCountersHashMap = new HashMap<String, String>();
		taskCountersHashMap = new HashMap<String, String>();
		hiveCountersHashMap = new HashMap<String, String>();
		hiveVertexCountersHashMap = new HashMap<String, HashMap<String, String>>();
		aggregatedInfo = new HashMap<String, Long>();
		aggregatedInfoKeys = new LinkedList<String>();
		dagParsingComplete = false;
	}

	public List<String> GetaggregatedInfoKeys() {
		return aggregatedInfoKeys;
	}

	public String getDAG_FINISHED() {
		return dagFinishedTime;
	}

	public String getDAG_INITIALIZED() {
		return dagInitializedTime;
	}

	public String getDAG_STARTED() {
		return dagStartedTime;
	}

	public String getDAG_SUBMITTED() {
		return dagSubmittedTime;
	}

	public String getDagSummaryHeader() {

		String header = "ApplicationId,DagId,DagInitializedTime,StartedTime,SubmittedTime,FinishedTime,Name,Diagnostics,EndTime,StartTime,Status,TimeTaken";

		for (String key : dagCountersHashMap.keySet()) {
			header += "," + key;
		}

		for (String key : fileSystemCountersHashMap.keySet()) {
			header += "," + key;
		}

		for (String key : taskCountersHashMap.keySet()) {
			header += "," + key;
		}

		for (String key : hiveCountersHashMap.keySet()) {
			header += "," + key;
		}

		for (String key : aggregatedInfo.keySet()) {
			header += "," + key;
		}

		return header;
	}

	public String getDagApplicationId() {
		return dagApplicationId;
	}

	public String getDagSummaryValues() {
		String values = dagApplicationId + "," + dagEntity + ","
				+ dagInitializedTime + "," + dagStartedTime + ","
				+ dagSubmittedTime + "," + dagFinishedTime + "," + m_dagName
				+ "," + dagDiagnostics + "," + dagEndTime + "," + dagStartTime
				+ "," + dagStatus + "," + dagTimeTaken;

		for (String key : dagCountersHashMap.keySet()) {
			values += "," + dagCountersHashMap.get(key);
		}

		for (String key : fileSystemCountersHashMap.keySet()) {
			values += "," + fileSystemCountersHashMap.get(key);
		}

		for (String key : taskCountersHashMap.keySet()) {
			values += "," + taskCountersHashMap.get(key);
		}

		for (String key : hiveCountersHashMap.keySet()) {
			values += "," + hiveCountersHashMap.get(key);
		}

		for (Long key : aggregatedInfo.values()) {
			values += "," + key.toString();
		}

		return values;
	}

	public String getEntity() {
		return dagEntity;
	}

	public Vertex GetVertexByEntity(String entityName) {
		Vertex matchingVertex = null;

		for (String currentVertexName : verticesHashMap.keySet()) {
			Vertex currentVertex = verticesHashMap.get(currentVertexName);
			String currentVertexEntity = currentVertex.getVertexEntity();

			if (currentVertexEntity == null) {
				continue;
			}
			if (currentVertexEntity.equals(entityName)) {
				matchingVertex = currentVertex;
				break;
			}
		}

		return matchingVertex;
	}

	public Vertex GetVertexByName(String name) {
		return verticesHashMap.get(name);
	}

	public void HandleDagEvents(JSONArray ja) {
		String currentEvent = ja.getJSONObject(0).getString(EVENT_TYPE);

		if (currentEvent.equals(EVENT_TYPES.DAG_INITIALIZED.toString())) {
			dagInitializedTime = ja.getJSONObject(0).get(TS).toString();
		}

		if (currentEvent.equals(EVENT_TYPES.DAG_STARTED.toString())) {
			dagStartedTime = ja.getJSONObject(0).get(TS).toString();
		}

		if (currentEvent.equals(EVENT_TYPES.DAG_SUBMITTED.toString())) {
			dagSubmittedTime = ja.getJSONObject(0).get(TS).toString();
		}

		if (currentEvent.equals(EVENT_TYPES.DAG_FINISHED.toString())) {
			dagFinishedTime = ja.getJSONObject(0).get(TS).toString();
		}
	}

	public boolean handleSubmitted() {
		return ((dagSubmittedTime != null) && (dagFinishedTime == null));
	}

	public void HandleTaskEvents(JSONObject jo) {

		String currentEvent = jo.getJSONArray(EVENTS).getJSONObject(0)
				.get(EVENT_TYPE).toString();

		if (currentEvent.equals(EVENT_TYPES.TASK_STARTED.toString())) {
			String currentVertexName = jo.getJSONArray(RELATED_ENTITIES)
					.getJSONObject(0).get(ENTITIY).toString();
			Vertex parentVertex = GetVertexByEntity(currentVertexName);
			Task currentTask = new Task(parentVertex, jo);
			parentVertex.AddTasktoTaskList(currentTask);
			taskIdObjectMap.put(currentTask.getTaskId(), currentTask);
		}

		if (currentEvent.equals(EVENT_TYPES.TASK_ATTEMPT_STARTED.toString())) {
			// Logs are not structured so need to lookup the actual task for the
			// given task ID
			String currentTaskId = jo.getJSONArray(RELATED_ENTITIES)
					.getJSONObject(2).get(ENTITIY).toString();
			Task currentTask = taskIdObjectMap.get(currentTaskId);
			currentTask.HandleAttemptEvent(jo);
		}

		if (currentEvent.equals(EVENT_TYPES.TASK_FINISHED.toString())) {
			String currentTaskId = jo.get("entity").toString();
			Task currentTask = taskIdObjectMap.get(currentTaskId);
			currentTask.HandleFinishedEvent(jo);
		}
	}

	public void HandleVertexEvents(JSONObject jo) {

		String currentEvent = jo.getJSONArray(EVENTS).getJSONObject(0)
				.get(EVENT_TYPE).toString();
		String currentVertexEntity = jo.get(ENTITIY).toString();

		if (currentEvent.equals(EVENT_TYPES.VERTEX_INITIALIZED.toString())) {
			String currentVertexName = jo.getJSONObject(OTHER_INFO)
					.get(VERTEX_NAME).toString();
			Vertex currentVertex = GetVertexByName(currentVertexName);
			currentVertex.HandleInitializedEvent(jo, this);
		}

		if (currentEvent.equals(EVENT_TYPES.VERTEX_STARTED.toString())) {
			Vertex currentVertex = GetVertexByEntity(currentVertexEntity);
			currentVertex.HandleStartedEvent(jo, this);
		}

		if (currentEvent.equals(EVENT_TYPES.VERTEX_FINISHED.toString())) {
			Vertex currentVertex = GetVertexByEntity(currentVertexEntity);
			if (currentVertex != null) {
				currentVertex.HandleFinishedEvent(jo);
			}
		}

	}

	public boolean isDagParsingComplete() {
		return dagParsingComplete;
	}

	public void PrintTaskSummary(List<String> aggregatedInfoKeys) {

		for (String currentVertexName : verticesHashMap.keySet()) {
			Vertex currentVertex = verticesHashMap.get(currentVertexName);

			for (Task currentTask : currentVertex.GetTaskList()) {
				System.out.println(currentTask
						.getTaskValues(aggregatedInfoKeys));
			}
		}
	}

	public ArrayList<String> getTaskSummary(List<String> aggregatedInfoKeys) {

		ArrayList<String> taskSummary = new ArrayList<>();

		taskSummary.add("\n");
		taskSummary.add(Task.getTaskSummaryHeader(aggregatedInfoKeys));

		for (String currentVertexName : verticesHashMap.keySet()) {
			Vertex currentVertex = verticesHashMap.get(currentVertexName);

			for (Task currentTask : currentVertex.GetTaskList()) {
				taskSummary.add(currentTask.getTaskValues(aggregatedInfoKeys));
			}
		}

		return taskSummary;
	}

	public void PrintVertexSummary(List<String> aggregatedInfoKeys) {

		System.out.println("\n"
				+ Vertex.getVertexSummaryHeader(aggregatedInfoKeys));

		for (String currentVertexName : verticesHashMap.keySet()) {
			Vertex currentVertex = verticesHashMap.get(currentVertexName);

			System.out.println(currentVertex
					.getVertexValues(aggregatedInfoKeys));
		}
	}

	public ArrayList<String> getVertexSummary(List<String> aggregatedInfoKeys) {

		ArrayList<String> vertexSummary = new ArrayList<>();
		vertexSummary.add(Vertex.getVertexSummaryHeader(aggregatedInfoKeys));

		for (String currentVertexName : verticesHashMap.keySet()) {
			Vertex currentVertex = verticesHashMap.get(currentVertexName);
			vertexSummary
			.add(currentVertex.getVertexValues(aggregatedInfoKeys));
		}
		return vertexSummary;
	}

	public void setDAG_FINISHED(String dAG_FINISHED) {
		dagFinishedTime = dAG_FINISHED;
	}

	public void setDAG_INITIALIZED(String dAG_INITIALIZED) {
		dagInitializedTime = dAG_INITIALIZED;
	}

	public void setDAG_STARTED(String dAG_STARTED) {
		dagStartedTime = dAG_STARTED;
	}

	public void setDAG_SUBMITTED(String dAG_SUBMITTED) {
		dagSubmittedTime = dAG_SUBMITTED;
	}

	public void setEntity(String entity) {
		this.dagEntity = entity;
	}

	public void setOtherInfo(JSONObject jsonObject) {

		if (handleSubmitted()) {
			setOtherInfoSubmitted(jsonObject);
		} else {
			setOtherInfoFinished(jsonObject);
		}
	}

	public void handleRelatedEntities(JSONObject jsonObject) {

		if (jsonObject.has(RELATED_ENTITIES)) {
			JSONArray relatedObjectsJson = jsonObject
					.getJSONArray(RELATED_ENTITIES);

			for (int i = 0; i < relatedObjectsJson.length(); i++) {
				if (relatedObjectsJson.getJSONObject(i).getString(ENTITY_TYPE)
						.equals(APPLICATION_ID)) {
					dagApplicationId = relatedObjectsJson.getJSONObject(i)
							.getString(ENTITIY);
					break;
				}
			}
		}
	}

	public void setOtherInfoFinished(JSONObject jsonObject) {

		try {
			dagDiagnostics = jsonObject.getString(DIAGNOSTICS);
		} catch (Exception e) {
			System.out.println(jsonObject.toString());
		}

		dagStartTime = jsonObject.getLong(START_TIME);
		dagEndTime = jsonObject.getLong(END_TIME);
		dagTimeTaken = (int) jsonObject.get(TIME_TAKEN);
		dagStatus = jsonObject.getString(STATUS);

		if (jsonObject.getJSONObject("counters").has("counters"))

		{
			String jsonObjectType = jsonObject.getJSONObject("counters")
					.get("counterGroups").getClass().getName();
			JSONArray countersGroupsArray = new JSONArray();
			JSONObject countersGroupsObject = null;

			if (jsonObjectType.equals("org.json.JSONObject")) {
				countersGroupsObject = jsonObject.getJSONObject("counters")
						.getJSONObject("counterGroups");
			} else {
				countersGroupsArray = jsonObject.getJSONObject("counters")
						.getJSONArray("counterGroups");
			}

			if ((countersGroupsArray == null) && (countersGroupsObject != null)) {
				countersGroupsArray.put(countersGroupsObject);
			}

			for (int i = 0; i < countersGroupsArray.length(); i++) {
				JSONObject currentCountersSet = countersGroupsArray
						.getJSONObject(i);
				String currentGroupName = currentCountersSet
						.getString(COUNTER_GROUP_NAME);

				String objectType = currentCountersSet.get("counters")
						.getClass().getName();
				JSONArray countersArray = null;
				JSONObject countersObject = null;

				if (objectType.equals("org.json.JSONObject")) {
					countersObject = currentCountersSet.getJSONObject(COUNTERS);
				} else {
					countersArray = currentCountersSet.getJSONArray(COUNTERS);
				}

				switch (currentGroupName) {

				// Parse dag counters such as TOTAL_LAUNCHED_TASKS,
				// DATA_LOCAL_TASKS
				// and RACK_LOCAL_TASKS
				case TEZ_DAG_COUNTERS: {
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
				case TASK_COUNTER: {
					Utils.parseKeyValuePairs(countersArray, countersObject,
							taskCountersHashMap);
				}
				;
				break;

				// Parse file systems counters such as CREATED_FILES
				case HIVE_COUNTER: {
					Utils.parseKeyValuePairs(countersArray, countersObject,
							hiveCountersHashMap);
				}
				;
				break;

				default: {
					HashMap<String, String> taskCountersMap = new HashMap<String, String>();
					Utils.parseKeyValuePairs(countersArray, countersObject,
							taskCountersMap);
					hiveVertexCountersHashMap.put(currentGroupName,
							taskCountersMap);
				}
				break;
				}
			}
		}

		aggregatedInfo = Utils.AggregateTaskCounters(hiveVertexCountersHashMap);

		// Save all the keys as they are needed for printing the Vertex and task
		// counters
		aggregatedInfoKeys.addAll(aggregatedInfo.keySet());

		dagParsingComplete = true;
	}

	// Parse the Edge and Vertex info
	void setOtherInfoSubmitted(JSONObject jo)

	{
		if (jo.has(VERTEX_ID_MAPPING)) {
			return;
		}

		for (int i = 0; i < jo.getJSONObject(DAG_PLAN).getJSONArray(VERTICES)
				.length(); i++) {
			Vertex currentVertex = new Vertex(jo.getJSONObject(DAG_PLAN)
					.getJSONArray(VERTICES).getJSONObject(i));
			verticesHashMap.put(currentVertex.getVertexName(), currentVertex);
		}

		if (jo.getJSONObject(DAG_PLAN).has(EDGES)) {

			String objectType = jo.getJSONObject(DAG_PLAN).get(EDGES).getClass()
					.getName();
			JSONArray edgesArray = new JSONArray();
			JSONObject edgesObject = null;

			if (objectType.equals("org.json.JSONObject")) {
				edgesObject = jo.getJSONObject(DAG_PLAN).getJSONObject(EDGES);
			} else {
				edgesArray = jo.getJSONObject(DAG_PLAN).getJSONArray(EDGES);
			}

			if (edgesArray.length() == 0) {
				edgesArray.put(edgesObject);
			}

			for (int i = 0; i < edgesArray.length(); i++) {

				Edge currentEdge = new Edge(edgesArray.getJSONObject(i));
				edgesHashMap.put(currentEdge.getEdgeId(), currentEdge);
				currentEdge.setInputVertex(verticesHashMap.get(currentEdge
						.getInputVertexName()));
				currentEdge.setOutputVertex(verticesHashMap.get(currentEdge
						.getOutputVertex()));
			}
		}

		// Now set the edges in the vertices
		for (Vertex currentVertex : verticesHashMap.values()) {
			// The vertex can have multiple inputs but one output

			// Save the input edges
			for (String edgeId : currentVertex.getInEdgeIds()) {
				currentVertex.AddEdgetoInputList(edgesHashMap.get(edgeId));
			}

			// Save the out edge
			currentVertex.SetVertexOutEdge(edgesHashMap.get(currentVertex
					.getOutEdgeId()));
		}

	}

	@Override
	public String toString() {
		return "tezdag [dagInitializedTime=" + dagInitializedTime
				+ ", dagStartedTime=" + dagStartedTime + ", dagSubmittedTime="
				+ dagSubmittedTime + ", dagFinishedTime=" + dagFinishedTime
				+ ", entity=" + dagEntity + ", m_dagName=" + m_dagName
				+ ", dagDiagnostics=" + dagDiagnostics + ", dagEndTime="
				+ dagEndTime + ", dagStartTime=" + dagStartTime
				+ ", dagStatus=" + dagStatus + ", dagTimeTaken=" + dagTimeTaken
				+ ", edgesHashMap=" + edgesHashMap + ", verticesHashMap="
				+ verticesHashMap + "]";
	}

}
