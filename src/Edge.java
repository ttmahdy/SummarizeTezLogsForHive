import org.json.JSONObject;

public class Edge {

	String dataMovementType;
	String dataSourceType;
	String edgeDestinationClass;
	String edgeId;
	String edgeSourceClass;
	String outputVertexName;
	String schedulingType;
	String tableSource;
	String inputVertexName;
	final String DATA_MOVEMENT_TYPE = "dataMovementType";
	final String DATA_SOURCE_TYPE = "dataSourceType";
	final String EDGE_DESTINATION_CLASS = "edgeDestinationClass";
	final String EDGE_ID = "edgeId";
	final String EDGE_SOURCE_CLASS = "edgeSourceClass";
	final String INPUT_VERTEX_NAME = "inputVertexName";
	final String OUTPUT_VERTEX_NAME = "outputVertexName";
	final String SCHEDULING_TYPE = "schedulingType";

	Vertex inputVertex;
	Vertex outputVertex;

	public Edge(JSONObject jo) {
		dataMovementType = jo.getString(DATA_MOVEMENT_TYPE);
		dataSourceType = jo.getString(DATA_SOURCE_TYPE);
		edgeDestinationClass = jo.getString(EDGE_DESTINATION_CLASS);
		edgeId = jo.getString(EDGE_ID);
		edgeSourceClass = jo.getString(EDGE_SOURCE_CLASS);
		inputVertexName = jo.getString(INPUT_VERTEX_NAME);
		outputVertexName = jo.getString(OUTPUT_VERTEX_NAME);
		schedulingType = jo.getString(SCHEDULING_TYPE);
	}

	public String getDataMovementType() {
		return dataMovementType;
	}

	public String getDataSourceType() {
		return dataSourceType;
	}

	public String getEdgeDestinationClass() {
		return edgeDestinationClass;
	}

	public String getEdgeId() {
		return edgeId;
	}

	public String getEdgeSourceClass() {
		return edgeSourceClass;
	}

	public Vertex getInputVertex() {
		return inputVertex;
	}

	public String getInputVertexName() {
		return inputVertexName;
	}

	public Vertex getOutputVertex() {
		return outputVertex;
	}

	public String getOutputVertexName() {
		return outputVertexName;
	}

	public String getSchedulingType() {
		return schedulingType;
	}

	public String getTableSource() {
		return tableSource;
	}

	public void setInputVertex(Vertex inputVertex) {
		this.inputVertex = inputVertex;
		if (inputVertex.getInputs() != null) {
			this.tableSource = inputVertex.getInputs().getName();
		}
	}

	public void setOutputVertex(Vertex outputVertex) {
		this.outputVertex = outputVertex;
	}

	public void setTableSource(String tableSource) {
		this.tableSource = tableSource;
	}

	@Override
	public String toString() {
		return "Edge [dataMovementType=" + dataMovementType
				+ ", dataSourceType=" + dataSourceType
				+ ", edgeDestinationClass=" + edgeDestinationClass
				+ ", edgeId=" + edgeId + ", edgeSourceClass=" + edgeSourceClass
				+ ", inputVertexName=" + inputVertexName
				+ ", outputVertexName=" + outputVertexName
				+ ", schedulingType=" + schedulingType + "]";
	}
}
