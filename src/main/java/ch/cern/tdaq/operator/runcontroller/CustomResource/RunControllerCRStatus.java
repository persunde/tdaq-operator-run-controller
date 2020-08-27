package ch.cern.tdaq.operator.runcontroller.CustomResource;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.KubernetesResource;

@JsonDeserialize(
        using = JsonDeserializer.None.class
)
public class RunControllerCRStatus implements KubernetesResource {
    private boolean isRunFinished;

    public boolean getIsRunFinished() { return isRunFinished; }
    public void setRunFinished(boolean runFinished) { isRunFinished = runFinished; }
}
