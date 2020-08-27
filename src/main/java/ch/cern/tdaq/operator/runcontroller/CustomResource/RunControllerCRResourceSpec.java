package ch.cern.tdaq.operator.runcontroller.CustomResource;


import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(
        using = JsonDeserializer.None.class
)
public class RunControllerCRResourceSpec implements KubernetesResource {
    private String name;
    private int runNumber;
    private String runPipe;

    public int getRunNumber() { return runNumber; }
    public void setRunNumber(int runNumber) { this.runNumber = runNumber; }

    public String getRunPipe() { return runPipe; }
    public void setRunPipe(String runPipe) { this.runPipe = runPipe; }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
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

