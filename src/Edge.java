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

	Vertex inputVertex;
	Vertex outputVertex;

	public Edge(JSONObject jo) {
    	dataMovementType = jo.getString("dataMovementType");
        dataSourceType = jo.getString("dataSourceType");
        edgeDestinationClass = jo.getString("edgeDestinationClass");
        edgeId = jo.getString("edgeId");
        edgeSourceClass = jo.getString("edgeSourceClass");
        inputVertexName = jo.getString("inputVertexName");
        outputVertexName = jo.getString("outputVertexName");
        schedulingType = jo.getString("schedulingType");
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
		if (inputVertex.getInputs() != null)
		{
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
