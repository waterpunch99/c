package lab;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;

public class ParseOperator extends RichMapFunction<String, Event> {
    private final String experiment, run;
    private transient ObjectMapper mapper;
    private transient Counter errors;
    public ParseOperator(String experiment, String run) { this.experiment = experiment; this.run = run; }
    @Override public void open(OpenContext ctx) {
        mapper = new ObjectMapper();
        errors = getRuntimeContext().getMetricGroup().counter("parseErrors");
    }
    public static Event parse(ObjectMapper mapper, String json, String experiment, String run) throws Exception {
        Event e = mapper.readValue(json, Event.class);
        e.validate(experiment, run);
        return e;
    }
    @Override public Event map(String value) throws Exception {
        try { return parse(mapper, value, experiment, run); }
        catch (Exception ex) { errors.inc(); throw ex; } // fail visibly; never quietly count malformed records
    }
}
