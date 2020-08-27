package ch.cern.tdaq.operator.runcontroller;

import ch.cern.tdaq.operator.runcontroller.CustomResource.DoneableRunControllerCR;
import ch.cern.tdaq.operator.runcontroller.CustomResource.RunControllerCRList;
import ch.cern.tdaq.operator.runcontroller.CustomResource.RunControllerCRResourceSpec;
import ch.cern.tdaq.operator.runcontroller.CustomResource.RunControllerCustomResource;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class RunControllerCustomResourceHelper {
    private static final String METADATA_LABEL_TDAQ_WORKER_KEY = "tdaq.worker";
    private static final String METADATA_LABEL_TDAQ_WORKER_VALUE = "true";

    private static final String crdName = "runresource.operator.tdaq.cern.ch";

    private final KubernetesClient kubernetesClient;

    public RunControllerCustomResourceHelper(KubernetesClient kubernetesClient) {
        ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(this.kubeConfigPath))).build();
        this.kubernetesClient = kubernetesClient;
        ers.Logger.info("Custom Resource Helper created"); /* How is ers even imported? It just works without importing it, since "ers" seems to be the fully qualified name? */
    }

    /**
     * Updates the RunController Custom Resource (CR)
     */
    public void updateRunControllerCustomResourceWithNewRun() {
        /**
         * !IMPORTANT NOTE: the crdName must match here and in the Operator!!!
         */
        CustomResourceDefinition runControllerCrd = kubernetesClient.customResourceDefinitions().withName(crdName).get();
        CustomResourceDefinitionContext context = CustomResourceDefinitionContext.fromCrd(runControllerCrd);

        MixedOperation<RunControllerCustomResource, RunControllerCRList, DoneableRunControllerCR, Resource<RunControllerCustomResource, DoneableRunControllerCR>> crClient = kubernetesClient
            .customResources(context, RunControllerCustomResource.class, RunControllerCRList.class, DoneableRunControllerCR.class);

        RunControllerCustomResource customResource = crClient.inNamespace("default").withName("runcontroller-cr").get(); /* TODO: fix how to get a generic runcontroler CR, name can change. Use label or something */
        RunControllerCRResourceSpec spec = customResource.getSpec();
        int nextRunNumber = spec.getRunNumber() + 1;
        spec.setRunNumber(nextRunNumber);

        /* TODO: set some status, like "LastUpdate" in the RunControllerCustomResource's status*/
        /* Update the CR with the new data aka new RunNumber */
        customResource = crClient.inNamespace("default").updateStatus(customResource);

        ers.Logger.info("Updated the RunController Custom Resource");
    }
}
