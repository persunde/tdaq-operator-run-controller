package ch.cern.tdaq.operator.runcontroller;

import ch.cern.tdaq.operator.runcontroller.CustomResource.DoneableRunControllerCR;
import ch.cern.tdaq.operator.runcontroller.CustomResource.RunControllerCRList;
import ch.cern.tdaq.operator.runcontroller.CustomResource.RunControllerCRResourceSpec;
import ch.cern.tdaq.operator.runcontroller.CustomResource.RunControllerCustomResource;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RunControllerCustomResourceHelper {
    private static final String METADATA_LABEL_TDAQ_WORKER_KEY = "tdaq.worker";
    private static final String METADATA_LABEL_TDAQ_WORKER_VALUE = "true";

    private static final String crdName = "runresources.operator.tdaq.cern.ch";
    final static String RUN_NUMBER_MAP_KEY = "runNumber";
    final static String RUN_CONTROLLER_CR_NAME = "runcontroller-cr";

    private final KubernetesClient kubernetesClient;

    public RunControllerCustomResourceHelper(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
        ers.Logger.info("Custom Resource Helper created"); /* How is ers even imported? It just works without importing it, since "ers" seems to be the fully qualified name? */
    }

    /**
     * Increments the runNumber in the RunController CR and updates it
     */
    public void updateRunControllerCustomResourceWithNewRun() throws IOException {
        CustomResourceDefinition runControllerCrd = kubernetesClient.customResourceDefinitions().withName(crdName).get();
        CustomResourceDefinitionContext context = CustomResourceDefinitionContext.fromCrd(runControllerCrd);
        /**
         * Note: CR's can be cluster wide or in a given namespace. If we want to use more than one namespace, we need to get the correct namespace here
         */
        /**
         * This did not work for some reason...??? I have to use the "Typeless API" instead of the "Typed API"
         */
//        MixedOperation<RunResource, RunResourceList, DoneableRunResource, Resource<RunResource, DoneableRunResource>> crClient = kubernetesClient
//                .customResources(context, RunResource.class, RunResourceList.class, DoneableRunResource.class);
//        RunResource customResource = crClient.inNamespace("default").withName("runcontroller-cr").get(); /* TODO: fix how to get a generic runcontroller CR, name can change. Use label or something */
//        RunResourceSpec spec = customResource.getSpec();
//        int nextRunNumber = spec.getRunNumber() + 1;
//        spec.setRunNumber(nextRunNumber);

        /* TODO: set some status, like "LastUpdate" in the RunControllerCustomResource's status*/
        /* Update the CR with the new data aka new RunNumber */
        /* customResource = crClient.inNamespace("default").updateStatus(customResource); */

        //crClient.createOrReplace(customResource);
        //crClient.updateStatus(customResource);

        Map<String, Object> runcontrollerCR = kubernetesClient.customResource(context).get("default", RUN_CONTROLLER_CR_NAME);

        int newRunNumber = 1 + (int) ((HashMap<String, Object>) runcontrollerCR.get("spec")).getOrDefault(RUN_NUMBER_MAP_KEY, -1); /* Not yet tested */
        ((HashMap<String, Object>)runcontrollerCR.get("spec")).put(RUN_NUMBER_MAP_KEY, newRunNumber);
        runcontrollerCR = kubernetesClient.customResource(context).edit("default", RUN_CONTROLLER_CR_NAME, new ObjectMapper().writeValueAsString(runcontrollerCR));

        ers.Logger.info("Updated the RunController Custom Resource");
    }

    /**
     * Updates the RunController Custom Resource (CR)
     */
    public void updateRunControllerCustomResourceWithNewRunOld() {
        /**
         * !IMPORTANT NOTE: the crdName must match here, in the Operator and in the CRD yaml file!!!
         */
        CustomResourceDefinition runControllerCrd = kubernetesClient.customResourceDefinitions().withName(crdName).get();
        CustomResourceDefinitionContext context = CustomResourceDefinitionContext.fromCrd(runControllerCrd);

        MixedOperation<RunControllerCustomResource, RunControllerCRList, DoneableRunControllerCR, Resource<RunControllerCustomResource, DoneableRunControllerCR>> crClient = kubernetesClient
            .customResources(context, RunControllerCustomResource.class, RunControllerCRList.class, DoneableRunControllerCR.class);

        /**
         * Note: CR's can be cluster wide or in a given namespace. If we want to use more than one namespace, we need to get the correct namespace here
         */
        RunControllerCustomResource customResource = crClient.inNamespace("default").withName("runcontroller-cr").get(); /* TODO: fix how to get a generic runcontroller CR, name can change. Use label or something? */
        RunControllerCRResourceSpec spec = customResource.getSpec();
        int nextRunNumber = spec.getRunNumber() + 1;
        spec.setRunNumber(nextRunNumber);

        /* TODO: set some status, like "LastUpdate" in the RunControllerCustomResource's status*/
        /* Update the CR with the new data aka new RunNumber */
        customResource = crClient.inNamespace("default").updateStatus(customResource);

        ers.Logger.info("Updated the RunController Custom Resource");
    }
}
