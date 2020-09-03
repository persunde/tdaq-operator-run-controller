package ch.cern.tdaq.operator.runcontroller.CustomResource;


import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(
        using = JsonDeserializer.None.class
)
public class RunControllerCRResourceSpec implements KubernetesResource {
    private String name;
    private long runNumber;
    private String runPipe;
    private String label;

    public long getRunNumber() { return runNumber; }
    public void setRunNumber(long runNumber) { this.runNumber = runNumber; }

    public String getRunPipe() { return runPipe; }
    public void setRunPipe(String runPipe) { this.runPipe = runPipe; }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    public String getLabel() {
        return label;
    }
    public void setLabel(String label) {
        this.label = label;
    }

    /* TOOD: Should implement custom toString() as seen below for CronTab example
    @Override
    public String toString() {
        return "CronTabSpec{" +
                "replicas=" + replicas  +
                ", cronSpec='" + cronSpec + "'" +
                ", image='" + image + "'" +
                "}";
    }

     */
}

