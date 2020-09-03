package ch.cern.tdaq.operator.runcontroller;

import ch.cern.tdaq.operator.runcontroller.CustomResource.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
        long nextRunNumber = spec.getRunNumber() + 1;
        spec.setRunNumber(nextRunNumber);

        /* TODO: set some status, like "LastUpdate" in the RunControllerCustomResource's status*/
        /* Update the CR with the new data aka new RunNumber */
        customResource = crClient.inNamespace("default").updateStatus(customResource);

        ers.Logger.info("Updated the RunController Custom Resource");
    }

    /**
     * 1. Get RunNumber
     * 2. Check if Parition-Name namespace exists if not:
     * 2.1 Create partition-namespace
     * 3. Get CR with this RunNumber in namespace from 2, if CR does not exist:
     * 3.1 Create new CR with the new RunNumber
     */

    public void createNewCR(String namespace, String customResourceName, long runNumber) {
        CustomResourceDefinition runControllerCrd = kubernetesClient.customResourceDefinitions().withName(crdName).get();
        CustomResourceDefinitionContext context = CustomResourceDefinitionContext.fromCrd(runControllerCrd);
//        Map<String, Object> runcontrollerCR = kubernetesClient.customResource(context).get(namespace, RUN_CONTROLLER_CR_NAME);
//        Map<String, Object> customResources = kubernetesClient.customResource(context).list(namespace);

        MixedOperation<RunControllerCustomResource, RunControllerCRList, DoneableRunControllerCR, Resource<RunControllerCustomResource, DoneableRunControllerCR>> crClient = kubernetesClient
                .customResources(context, RunControllerCustomResource.class, RunControllerCRList.class, DoneableRunControllerCR.class);

        RunControllerCRResourceSpec spec = new RunControllerCRResourceSpec();
        spec.setName(customResourceName);
        spec.setRunNumber(runNumber);
        spec.setRunPipe("proton");

        RunControllerCRStatus status = new RunControllerCRStatus();
        status.setRunFinished(false);

        RunControllerCustomResource runControllerCR = new RunControllerCustomResource();
        runControllerCR.setSpec(spec);
        runControllerCR.setStatus(status);

        ObjectMeta metadata = runControllerCR.getMetadata();
        if (metadata == null) {
            metadata = new ObjectMeta();
            runControllerCR.setMetadata(metadata);
        }
        metadata.setNamespace(namespace);
        metadata.setName(customResourceName);

        crClient.inNamespace(namespace).create(runControllerCR);
    }

    /**
     * Creates a new namespace if it does not already exist.
     * This should be used to create a namespace for each partition running in the cluster.
     *
     * @param namespaceName The name of the namespace. It should be the name of the related segment
     */
    public void createNamespaceIfNotExists(String namespaceName) throws FileNotFoundException {
        Namespace namespace = kubernetesClient.namespaces().withName(namespaceName).get();
        if (namespace == null || namespaceName.isEmpty()) {
            HashMap<String, String> labels = new HashMap<>();
            labels.put("name", namespaceName);
            namespace = new NamespaceBuilder()
                    .withNewMetadata()
                    .withName(namespaceName)
                    .withLabels(labels)
                    .endMetadata()
                    .build();
            /* Namespace namespaceObj = kubernetesClient.namespaces().load(new FileInputStream("namespace-test.yml")).get(); */
            kubernetesClient.namespaces().create(namespace);
        }
    }
}
