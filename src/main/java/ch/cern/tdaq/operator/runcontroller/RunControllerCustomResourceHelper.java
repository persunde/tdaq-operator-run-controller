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

    final private static String CRD_NAME = "runresources.operator.tdaq.cern.ch";
    final public static String CR_KIND = "RunResource";
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
        CustomResourceDefinition runControllerCrd = kubernetesClient.customResourceDefinitions().withName(CRD_NAME).get();
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
         * !IMPORTANT NOTE: the CRD_NAME must match here, in the Operator and in the CRD yaml file!!!
         */
        CustomResourceDefinition runControllerCrd = kubernetesClient.customResourceDefinitions().withName(CRD_NAME).get();
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

    public void createOrUpdateCustomResource(String partitionName, String runType, long runNumber) {
        /**
         * TODO: create a filter function that hopefully creates DNS (RFC 1123) compatible names
         * This should be a valid RegEx to fix the issue:
         * https://stackoverflow.com/questions/2063213/regular-expression-for-validating-dns-label-host-name/2063247#2063247
         */
        partitionName = partitionName.replace("_", "-").toLowerCase();
        runType = runType.replace("_", "-").toLowerCase();

        String filteredNamespace = createNamespaceIfNotExists(partitionName);
        CustomResourceDefinition runControllerCrd = kubernetesClient.customResourceDefinitions().withName(CRD_NAME).get();
        CustomResourceDefinitionContext context = CustomResourceDefinitionContext.fromCrd(runControllerCrd);
//        Map<String, Object> runcontrollerCR = kubernetesClient.customResource(context).get(namespace, RUN_CONTROLLER_CR_NAME);
//        Map<String, Object> customResources = kubernetesClient.customResource(context).list(namespace);

        String customResourceName = getCustomResourceName(filteredNamespace, runType, runNumber);
        MixedOperation<RunControllerCustomResource, RunControllerCRList, DoneableRunControllerCR, Resource<RunControllerCustomResource, DoneableRunControllerCR>> crClient = kubernetesClient
                .customResources(context, RunControllerCustomResource.class, RunControllerCRList.class, DoneableRunControllerCR.class);

        RunControllerCustomResource customResource = crClient.inNamespace(filteredNamespace).withName(customResourceName).get();
        if (customResource == null) {
            RunControllerCRResourceSpec spec = new RunControllerCRResourceSpec();
            spec.setName(customResourceName);
            spec.setRunNumber(runNumber);
            spec.setRunPipe(runType);
            spec.setLabel(partitionName.replace("_", "-").toLowerCase());

            RunControllerCRStatus status = new RunControllerCRStatus();
            status.setRunFinished(false);

            RunControllerCustomResource runControllerCR = new RunControllerCustomResource();
            runControllerCR.setKind(CR_KIND);
            runControllerCR.setSpec(spec);
            runControllerCR.setStatus(status);

            ObjectMeta metadata = runControllerCR.getMetadata();
            if (metadata == null) {
                metadata = new ObjectMeta();
                runControllerCR.setMetadata(metadata);
            }
            metadata.setNamespace(filteredNamespace);
            metadata.setName(customResourceName);

            crClient.inNamespace(filteredNamespace).create(runControllerCR);
        } else {
            RunControllerCRResourceSpec spec = customResource.getSpec();
            spec.setName(customResourceName);
            spec.setRunNumber(runNumber);
            spec.setRunPipe(runType);

            customResource.getStatus().setRunFinished(false);

            ObjectMeta metadata = customResource.getMetadata();
            metadata.setNamespace(filteredNamespace);
            metadata.setName(customResourceName);

            crClient.inNamespace(filteredNamespace).updateStatus(customResource); /* This does not actually work for some reason */
        }
    }

    private String getCustomResourceName(String partitionName, String runType, long runNumber) {
        final int runNumberPaddingSize = 4;
        String formattedRunNumber = String.format("%0" + runNumberPaddingSize + "d", runNumber);
        String fullNewName = partitionName + "-" + runType + "-" + formattedRunNumber;
        return fullNewName.toLowerCase();
    }

    /**
     * Creates a new namespace if it does not already exist.
     * This should be used to create a namespace for each partition running in the cluster.
     *
     * See: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
     * NOTE: Restrictions on the name:
     *     contain no more than 253 characters (sometimes 64 characters)
     *     contain only lowercase alphanumeric characters, '-' or '.'
     *     start with an alphanumeric character
     *     end with an alphanumeric character
     *
     * @param namespaceName The name of the namespace. It should be the name of the related segment
     * @return the filtered namespace
     */
    public String createNamespaceIfNotExists(String namespaceName) {
        namespaceName = namespaceName.replace("_", "-").toLowerCase();
        Namespace namespace = kubernetesClient.namespaces().withName(namespaceName).get();
        if (namespace == null) {
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
        return namespaceName;
    }
}
