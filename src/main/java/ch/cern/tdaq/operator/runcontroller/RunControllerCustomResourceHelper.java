package ch.cern.tdaq.operator.runcontroller;

import ch.cern.tdaq.operator.runcontroller.CustomResource.*;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

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
     * Deletes a CustomResource that is named after the parameters given.
     * If unable to delete the CR, it will sleeps for 5 seconds before it will try again.
     * Throws an IOException if it is unable to delete the CR after five tries.
     * @param partitionName
     * @param runType
     * @param runNumber
     * @throws IOException Throws if it is unable to delete the CR within 5 tries.
     */
    public void deleteCustomResource(String partitionName, String runType, long runNumber) throws IOException {
        partitionName = makeStringDNSCompatible(partitionName);
        runType = makeStringDNSCompatible(runType);
        String filteredNamespace = makeStringDNSCompatible(partitionName);
        String customResourceName = getCustomResourceName(filteredNamespace, runType, runNumber);

        CustomResourceDefinition runControllerCrd = kubernetesClient.customResourceDefinitions().withName(CRD_NAME).get();
        CustomResourceDefinitionContext context = CustomResourceDefinitionContext.fromCrd(runControllerCrd);
        MixedOperation<RunControllerCustomResource, RunControllerCRList, DoneableRunControllerCR, Resource<RunControllerCustomResource, DoneableRunControllerCR>> crClient = kubernetesClient
                .customResources(context, RunControllerCustomResource.class, RunControllerCRList.class, DoneableRunControllerCR.class);

        Boolean deleted = false;
        int count = 0;
        while (!deleted) {
            deleted = crClient.inNamespace(filteredNamespace).withName(customResourceName).delete();
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (count++ >= 5) {
                throw new IOException("Failed to delete the CustomResource: " + customResourceName + " in Namespace: " + filteredNamespace);
            }
        }
    }

    /**
     * Creates a new CR in the cluster if it does not already exist and creates a namespace named after the Parition,
     * if it does not already exist.
     * Changes the strings be DNS compatible.
     * @param partitionName
     * @param runType
     * @param runNumber
     */
    public void createCustomResourceIfNotExist(String partitionName, String runType, long runNumber) {
        /**
         * TODO: create a filter function that hopefully creates DNS (RFC 1123) compatible names
         * This should be a valid RegEx to fix the issue:
         * https://stackoverflow.com/questions/2063213/regular-expression-for-validating-dns-label-host-name/2063247#2063247
         */
        partitionName = makeStringDNSCompatible(partitionName);
        runType = makeStringDNSCompatible(runType);
        String filteredNamespace = makeStringDNSCompatible(partitionName);

        createNamespaceIfNotExists(filteredNamespace);
        CustomResourceDefinition runControllerCrd = kubernetesClient.customResourceDefinitions().withName(CRD_NAME).get();
        CustomResourceDefinitionContext context = CustomResourceDefinitionContext.fromCrd(runControllerCrd);
//        Map<String, Object> runcontrollerCR = kubernetesClient.customResource(context).get(namespace, RUN_CONTROLLER_CR_NAME);
//        Map<String, Object> customResources = kubernetesClient.customResource(context).list(namespace);

        String customResourceName = getCustomResourceName(filteredNamespace, runType, runNumber);
        MixedOperation<RunControllerCustomResource, RunControllerCRList, DoneableRunControllerCR, Resource<RunControllerCustomResource, DoneableRunControllerCR>> crClient = kubernetesClient
                .customResources(context, RunControllerCustomResource.class, RunControllerCRList.class, DoneableRunControllerCR.class);

        RunControllerCustomResource customResource = null;
        try {
            customResource = crClient.inNamespace(filteredNamespace).withName(customResourceName).get();
        } catch (NullPointerException e) {
            // Ignore
        }
        if (customResource == null) {
            RunControllerCRResourceSpec spec = new RunControllerCRResourceSpec();
            spec.setName(customResourceName);
            spec.setRunNumber(runNumber);
            spec.setRunPipe(runType);
            spec.setLabel(filteredNamespace);

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
        }
//        else {
//            RunControllerCRResourceSpec spec = customResource.getSpec();
//            spec.setName(customResourceName);
//            spec.setRunNumber(runNumber);
//            spec.setRunPipe(runType);
//
//            customResource.getStatus().setRunFinished(false);
//
//            ObjectMeta metadata = customResource.getMetadata();
//            metadata.setNamespace(filteredNamespace);
//            metadata.setName(customResourceName);
//
//            crClient.inNamespace(filteredNamespace).updateStatus(customResource); /* This does not actually work for some reason */
//        }
    }

    private String getCustomResourceName(String partitionName, String runType, long runNumber) {
        final int runNumberPaddingSize = 10;
        String formattedRunNumber = String.format("%0" + runNumberPaddingSize + "d", runNumber);
        String fullNewName = partitionName + "-" + runType + "-" + formattedRunNumber;
        return fullNewName.toLowerCase();
    }

    private String makeStringDNSCompatible(String text) {
        return text.replace("_", "-").toLowerCase();
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
    public void createNamespaceIfNotExists(String namespaceName) {
        namespaceName = makeStringDNSCompatible(namespaceName);
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
    }
}
