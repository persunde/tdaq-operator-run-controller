package ch.cern.tdaq.operator.runcontroller.CustomResource;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

public class DoneableRunControllerCR extends CustomResourceDoneable<RunControllerCustomResource> {
    public DoneableRunControllerCR(RunControllerCustomResource resource, Function function) { super(resource, function); }
}

