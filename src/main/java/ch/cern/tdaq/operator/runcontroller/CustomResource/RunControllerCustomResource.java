package ch.cern.tdaq.operator.runcontroller.CustomResource;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;

/**
 * NOTE: Impelementing "Namespaced" means it is a namespaced resource. If you remove it, the CR would be considered a Cluster scoped resource.
 */
public class RunControllerCustomResource extends CustomResource implements Namespaced {
    private RunControllerCRResourceSpec spec;
    private RunControllerCRStatus status;

    public RunControllerCRResourceSpec getSpec() {
        return spec;
    }
    public void setSpec(RunControllerCRResourceSpec spec) {
        this.spec = spec;
    }

    public RunControllerCRStatus getStatus() { return status; }
    public void setStatus(RunControllerCRStatus status) {
        this.status = status;
    }
}
