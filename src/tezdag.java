import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONObject;

public class tezdag {

	public enum EVENT_TYPES {
		DAG_FINISHED("DAG_FINISHED"), DAG_INITIALIZED("DAG_INITIALIZED"), DAG_STARTED("DAG_STARTED"), DAG_SUBMITTED(
						"DAG_SUBMITTED"),
		VERTEX_FINISHED("VERTEX_FINISHED"), VERTEX_INITIALIZED("VERTEX_INITIALIZED"),
		VERTEX_STARTED("VERTEX_STARTED");

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
	
	private static final String counterName = "counterName";
	
	private static final String counters = "counters";

	private static final String counterValue = "counterValue";
	private static final String dagCounters = "org.apache.tez.common.counters.DAGCounter";

	private static final String dagName = "dagName";

	private static final String dagPlan = "dagPlan";
	
	private static final String diagnostics = "diagnostics";

	private static final String edges = "edges";

	private static final String endTime = "endTime";

	private static final String entity = "entity";
	
	private static final String events = "events";
	
	private static final String eventtype = "eventtype";
	
	private static final String fileSystemCounter = "org.apache.tez.common.counters.FileSystemCounter";

	private static final String hivecounter = "HIVE";

	private static final String otherinfo = "otherinfo";

	private static final String startTime = "startTime";

	private static final String status = "status";

	private static final String taskCounter = "org.apache.tez.common.counters.TaskCounter";

	private static final String timeTaken = "timeTaken";

	private static final String ts = "ts";

	private static final String vertexName = "vertexName";

	private static final String vertices = "vertices";

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
	HashMap<String, Vertex> verticesHashMap;
	
	public tezdag() {
		super();
		edgesHashMap = new HashMap<String, Edge>();
		verticesHashMap = new HashMap<String, Vertex>();
		dagCountersHashMap = new HashMap<String, String>();
		fileSystemCountersHashMap = new HashMap<String, String>();
		taskCountersHashMap = new HashMap<String, String>();
		hiveCountersHashMap = new HashMap<String, String>();
		hiveVertexCountersHashMap = new HashMap<String, HashMap<String, String>>();
		dagParsingComplete = false;
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
	public String getDagSummaryHeader()
	{
		String header = "entity,dagInitializedTime,StartedTime,SubmittedTime,FinishedTime,Name,Diagnostics,EndTime,StartTime,Status,TimeTaken";
		
		for ( String key : dagCountersHashMap.keySet() )
		{
			header +="," + key;
		}
		
		for ( String key : fileSystemCountersHashMap.keySet() )
		{
			header +="," + key;
		}

		for ( String key : taskCountersHashMap.keySet() )
		{
			header +="," + key;
		}

		for ( String key : hiveCountersHashMap.keySet() )
		{
			header +="," + key;
		}
		
		return header;
	}
	
	public String getDagSummaryValues()
	{
		String values= dagEntity + ","+ dagInitializedTime + ","+ dagStartedTime + ","+ dagSubmittedTime + ","+ dagFinishedTime + ","+ m_dagName + ","+ dagDiagnostics + ","+ dagEndTime + ","+ dagStartTime + ","+ dagStatus + ","+ dagTimeTaken;
		
		for ( String key : dagCountersHashMap.keySet() )
		{
			values +="," + dagCountersHashMap.get(key);
		}
		
		for ( String key : fileSystemCountersHashMap.keySet() )
		{
			values +="," + fileSystemCountersHashMap.get(key);
		}

		for ( String key : taskCountersHashMap.keySet() )
		{
			values +="," + taskCountersHashMap.get(key);
		}

		for ( String key : hiveCountersHashMap.keySet() )
		{
			values +="," + hiveCountersHashMap.get(key);
		}
		
		return values;
	}
	
	public void PrintVertexSummary()
	{
		
		for (String currentVertexName : verticesHashMap.keySet())
		{
			Vertex currentVertex = verticesHashMap.get(currentVertexName);
			
			System.out.println(currentVertex.getVertexValues());
		}
	}
	
	public String getEntity() {
		return dagEntity;
	}
	public Vertex GetVertexByEntity (String entityName)
	{
		Vertex matchingVertex = null;
		
		for (String currentVertexName : verticesHashMap.keySet())
		{
			Vertex currentVertex = verticesHashMap.get(currentVertexName); 
			String currentVertexEntity = currentVertex.getVertexEntity(); 
			
			if (currentVertexEntity == null)
			{
				continue;
			}
			if (currentVertexEntity.equals(entityName))
			{
				matchingVertex = currentVertex;
				break;
			}
		}
		
		return matchingVertex;
	}
	public Vertex GetVertexByName (String name)
	{
		return verticesHashMap.get(name);
	}
	public void HandleDagEvents(JSONArray ja) {
		String currentEvent = ja.getJSONObject(0).getString(eventtype);

		if (currentEvent.equals(EVENT_TYPES.DAG_INITIALIZED.toString())) {
			dagInitializedTime = ja.getJSONObject(0).get(ts).toString();
		}

		if (currentEvent.equals(EVENT_TYPES.DAG_STARTED.toString())) {
			dagStartedTime = ja.getJSONObject(0).get(ts).toString();
		}

		if (currentEvent.equals(EVENT_TYPES.DAG_SUBMITTED.toString())) {
			dagSubmittedTime = ja.getJSONObject(0).get(ts).toString();
		}

		if (currentEvent.equals(EVENT_TYPES.DAG_FINISHED.toString())) {
			dagFinishedTime = ja.getJSONObject(0).get(ts).toString();
		}
	}
	public boolean handleSubmitted() {
		return ((dagSubmittedTime != null) && (dagFinishedTime == null));
	}
	public void HandleVertexEvents(JSONObject jo) { 
		
		
		String currentEvent =  jo.getJSONArray(events).getJSONObject(0).get(eventtype).toString();
		String currentVertexEntity = jo.get(entity).toString();
		
		if (currentEvent.equals(EVENT_TYPES.VERTEX_INITIALIZED.toString())) {
			String currentVertexName = jo.getJSONObject(otherinfo).get(vertexName).toString();
			Vertex currentVertex = GetVertexByName(currentVertexName);
			currentVertex.HandleInitializedEvent(jo, this);
		}

		if (currentEvent.equals(EVENT_TYPES.VERTEX_STARTED.toString())) {
			Vertex currentVertex = GetVertexByEntity(currentVertexEntity);
			currentVertex.HandleStartedEvent(jo, this);
		}

		if (currentEvent.equals(EVENT_TYPES.VERTEX_FINISHED.toString())) {
			//dagSubmittedTime = ja.getJSONObject(0).get(ts).toString();
			Vertex currentVertex = GetVertexByEntity(currentVertexEntity);
			currentVertex.HandleFinishedEvent(jo);
		}

	}
	public boolean isDagParsingComplete() {
		return dagParsingComplete;
	}
	public void parseKeyValuePairs(JSONArray ja, HashMap<String, String> hashmap )
	{
		 for (int j = 0 ; j < ja.length(); j++)
		 {
			 hashmap.put(ja.getJSONObject(j).getString(counterName), 
					 ja.getJSONObject(j).get(counterValue).toString());
		 }
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
		}
		else
		{
			setOtherInfoFinished(jsonObject);
		}
	}
	public void setOtherInfoFinished(JSONObject jsonObject)
	{
		dagDiagnostics  =  jsonObject.getString(diagnostics); 
		dagStartTime 	=  jsonObject.getLong(startTime);
		dagEndTime 		=   jsonObject.getLong(endTime);
		dagTimeTaken = (int) jsonObject.get(timeTaken);
		dagStatus 		= jsonObject.getString(status);
		
		JSONArray ja = jsonObject.getJSONObject("counters").getJSONArray("counterGroups");
		
		for (int i = 0 ; i < ja.length() ; i ++)
		{
			JSONObject currentCountersSet = ja.getJSONObject(i);
			String currentGroupName = currentCountersSet.getString(counterGroupName);
			JSONArray countersArray = currentCountersSet.getJSONArray(counters);
			
			switch (currentGroupName) {

				// Parse dag counters such as TOTAL_LAUNCHED_TASKS, DATA_LOCAL_TASKS and RACK_LOCAL_TASKS
				case dagCounters:  
				{
					parseKeyValuePairs(countersArray,dagCountersHashMap);
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
	
				// Parse file systems counters such as CREATED_FILES
				case hivecounter:  
				{
					parseKeyValuePairs(countersArray,hiveCountersHashMap);
				};
				break;
				
				default:
				{
					System.out.println(currentGroupName);
					HashMap<String, String> taskCountersMap = new HashMap<String, String>();
					parseKeyValuePairs(countersArray, taskCountersMap);
					hiveVertexCountersHashMap.put(currentGroupName, taskCountersMap);
				}
				break;
			}
		}
		
		dagParsingComplete = true;
	}
	// Parse the Edge and Vertex info
	void setOtherInfoSubmitted(JSONObject jo) 
	
	{
		m_dagName = (String) jo.getJSONObject(dagPlan).get(dagName);

		for (int i = 0; i < jo.getJSONObject(dagPlan).getJSONArray(vertices).length(); i++) {
			
			Vertex currentVertex = new Vertex(jo.getJSONObject(dagPlan)
					.getJSONArray(vertices).getJSONObject(i));
			
			verticesHashMap.put(currentVertex.getVertexName(), currentVertex);
			System.out.println(currentVertex.toString());
		}


		for (int i = 0; i < jo.getJSONObject(dagPlan).getJSONArray(edges).length(); i++) {
			
			Edge currentEdge = new Edge(jo.getJSONObject(dagPlan)
					.getJSONArray(edges).getJSONObject(i));
			edgesHashMap.put(currentEdge.getEdgeId(), currentEdge);
			
			currentEdge.setInputVertex(verticesHashMap.get(currentEdge
					.getInputVertexName()));
			
			currentEdge.setOutputVertex(verticesHashMap.get(currentEdge
					.getOutputVertex()));
			
			System.out.println(currentEdge.toString());
		}
		
		// Now set the edges in the vertices
		for (Vertex currentVertex: verticesHashMap.values())
		{
			// The vertex can have multiple inputs but one output
			
			// Save the input edges
			for (String edgeId : currentVertex.getInEdgeIds())
			{
				currentVertex.AddEdgetoInputList(edgesHashMap.get(edgeId));
			}
			
			// Save the out edge
			currentVertex.SetVertexOutEdge(edgesHashMap.get(currentVertex.getOutEdgeId()));
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
