package ch.cern.tdaq.operator.runcontroller;

import ch.cern.k8sjava.UserCmdParamsParser;
import ch.cern.k8sjava.types.K8SDeployment;
import ch.cern.k8sjava.types.K8SHorizontalPodAutoscaler;
import ch.cern.k8sjava.types.K8SType;
import ch.cern.k8sjava.types.K8STypeBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


import ch.cern.k8sjava.types.K8SDeployment;
import ch.cern.k8sjava.types.K8SHorizontalPodAutoscaler;
import ch.cern.k8sjava.types.K8SType;
import ch.cern.k8sjava.types.K8STypeBuilder;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.util.ClientBuilder;
import org.apache.commons.cli.ParseException;

import java.io.FileReader;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import io.kubernetes.client.util.KubeConfig;

import daq.rc.ApplicationState;
import daq.rc.Command.ResynchCmd;
import daq.rc.Command.SubTransitionCmd;
import daq.rc.Command.TransitionCmd;
import daq.rc.Command.UserCmd;
import daq.rc.CommandLineParser;
import daq.rc.CommandLineParser.HelpRequested;
import daq.rc.JControllable;
import daq.rc.JItemCtrl;
import daq.rc.OnlineServices;
import daq.rc.RCException.ItemCtrlException;
import daq.rc.RCException.OnlineServicesFailure;
import ers.Issue;

import k8sjava.K8SAbstractType;
import k8sjava.K8SAbstractType_Helper;
import k8sjava.KubernetesRCApplication;
import k8sjava.KubernetesRCApplication_Helper;

import config.Configuration;
import org.jetbrains.annotations.NotNull;



// TODO: have a Watch that updates a counter of number of Ready Pods. Then publish() will fetch that number and log it (preferably to IS, waiting for example code from Giuseppe)
/* TODO: add functionality to manually change the autoscaler, eg max/min pods, averageUtilization, scaleUp/Down -> type: (Pods|Percent) value */

public class KubeOperatorController {
    public static String partitionName = "";

    // Exception extending ers.Issue in order to report errors during state transitions
    static class TransitionFailure extends Issue {
        private static final long serialVersionUID = 7123663373144988455L;

        public TransitionFailure(final String message) {
            super(message);
        }

        public TransitionFailure(final String message, final Exception reason) {
            super(message, reason);
        }
    }

    // Exception extending ers.Issue in order to report errors during UserCmd failures
    static class UserCmdFailure extends Issue {
        private static final long serialVersionUID = 4223663373144988455L; // TODO: what is this, do I need this??

        public UserCmdFailure(final String message) {
            super(message);
        }

        public UserCmdFailure(final String message, final Exception reason) {
            super(message, reason);
        }
    }

    // This interface defines the actions the application will execute
    static class MyControllable implements JControllable {
        private HashMap<String, K8SType> k8STypeMap = new HashMap<>();
        private  config.Configuration db = null;
        final private String kubeConfigPath;
        private KubernetesClient kubernetesClient;

        MyControllable() throws Exception {
            super();
            initDb();

            ers.Logger.info("Will try to read from db with getYamlFilePathsFromOKS()\n");
            try {
                this.kubeConfigPath = getKubeconfigFilePathFromOKS();
                initK8SCluster(this.kubeConfigPath);
                initKubernetsTypesFromOKS();
            } catch (Exception e) {
                ers.Logger.log(e);
                e.printStackTrace();
                throw e;
            }
        }

        @Override
        public void publish() throws Issue {
            /*
            List<String> podsCpuCriticalUsageMessages = MetricsGatherer.getPodsCpuCriticalMessages("default");
            int i = 5;
            for (String msg : podsCpuCriticalUsageMessages) {
                ers.Logger.info(msg);
                if (--i == 0) {
                    break;
                }
            }
            ers.Logger.log("Publishing..."); //ers.Logger.info() .warning()
             */
        }

        @Override
        public void publishFullStats() throws Issue {
            ers.Logger.log("Publishing full stats...");
        }

        /**
         * 1. Deployment:
         *  1. Let users Start, Stop, Restart a Deployment
         *  2. Let users manually Scale Up or Down a Deployment
         * 2. Horizontal Pod Autoscaler (HPA):
         *  1. Let users set the new Max and Min Replicas of a HPA
         */
        @Override
        public void user(final UserCmd usrCmd) throws Issue {
            ers.Logger.log("Executed user command " + usrCmd.getCommandName());
            String commandName = usrCmd.getCommandName();
            try {
                String[] usrCmdParams = usrCmd.getParameters();
                UserCmdParamsParser cmdParser = new UserCmdParamsParser(usrCmdParams); // Parses the params/args for the UsrCmd for K8S-RunController

                /* Deployment actions */
                if (commandName.toLowerCase().equals("deploy")) {
                    String deployCommand = cmdParser.getDeployCommand();
                    String deploymentName = cmdParser.getDeploymentName();
                    boolean waitForCompletion = cmdParser.getWaitForCompletion();
                    if (deployCommand.equals("start")) {
                        int percentTarget = cmdParser.getPercentTarget();
                        startDeployment(deploymentName, waitForCompletion, percentTarget);
                    } else if (deployCommand.equals("stop")) {
                        stopDeployment(deploymentName, waitForCompletion);
                    } else if (deployCommand.equals("restart")) {
                        int percentTarget = cmdParser.getPercentTarget();
                        restartDeployment(deploymentName, waitForCompletion, percentTarget);
                    } else if (deployCommand.equals("scale")) {
                        scaleDeploymentAbsolute(deploymentName, cmdParser.getPodNumber());
                    }
                    ers.Logger.log("Executed user command " + commandName + " and deploymentCommand " + deployCommand);
                }
                /* HPA actions */
                else if (commandName.toLowerCase().equals("hpa")) {
                    String hpaCommand = cmdParser.getHpaCommand();
                    String hpaName = cmdParser.getHpaName();
                    if (hpaCommand.equals("minreplicas")) {
                        int minReplicas = cmdParser.getMinReplicas();
                        setHpaMinReplicas(hpaName, minReplicas);
                    } else if (hpaCommand.equals("maxreplicas")) {
                        int maxReplicas = cmdParser.getMaxReplicas();
                        setHpaMaxReplicas(hpaName, maxReplicas);
                    }
                    ers.Logger.log("Executed user command " + commandName + " and hpaCommand " + hpaCommand);
                }
                /* Wrong command given, no action taken */
                else {
                    /* NOTE: Should I throw an Exception here or not?  */
                    ers.Logger.info("Wrong command given! No actions were taken");
                }

            } catch (final Exception e) {
                // TODO: Should I throw an Exception, or just log a warning?
                StackTraceElement[] stack = e.getStackTrace();
                String errorMsg = "";
                for (StackTraceElement s : stack) {
                    errorMsg += s.getFileName() + " " + s.getMethodName() + " " + s.getLineNumber() + "\n";
                }
                ers.Logger.info("161 Exception + " + errorMsg + " :: " + e.getMessage() + " :: " + e.toString() + "\n");
                ers.Logger.warning(e);

                throw new ch.cern.k8sjava.KubeController.UserCmdFailure("Failed to complete the user command " + commandName + ". " + e.getMessage(), e);
            }
        }

        @Override
        public void enable(final List<String> components) throws Issue {
            ers.Logger.log("Enabled components " + Arrays.toString(components.toArray()));
        }

        @Override
        public void disable(final List<String> components) throws Issue {
            ers.Logger.log("Disabled components " + Arrays.toString(components.toArray()));
        }

        @Override
        public void onExit(final ApplicationState state) {
            ers.Logger.log("Exiting while in state " + state.name());
        }

        @Override
        public void configure(final TransitionCmd cmd) throws Issue {
            /* TODO: get the latest changes from the RunController gitlab master branch. This template code is taken from old commit, there is a new and better one! */
            try {
                /* Build the client that is used to interact with the Cluster */
                System.setProperty("KUBECONFIG", this.kubeConfigPath);
                Config config = new ConfigBuilder().withNamespace(null).build();
                this.kubernetesClient = new DefaultKubernetesClient(config);
            } catch (IOException e) {
                e.printStackTrace();
                throw new ch.cern.k8sjava.KubeController.TransitionFailure("Cannot read/access the kubeconfig file: ", e);
            }

            // Let's retrieve some information from the configuration
            try {
                final OnlineServices os = OnlineServices.instance();

                final String dbAppUID = os.getApplication().UID();
                final String appName = os.getApplicationName();

                System.out.println("This application name is " + appName);
                System.out.println("The UID of the corresponding application object in the DB is " + dbAppUID);
            }
            catch(final OnlineServicesFailure ex) {
                throw new ch.cern.k8sjava.KubeController.TransitionFailure("Cannot retrieve configuration information: " + ex.getMessage(), ex);
            }
            this.logTransition(cmd);
        }

        @Override
        public void connect(final TransitionCmd cmd) throws Issue {
            this.logTransition(cmd);
        }

        @Override
        public void prepareForRun(final TransitionCmd cmd) throws Issue {
            try {
                applyEverything();
            } catch (ApiException | IOException e) {
                throw new ch.cern.k8sjava.KubeController.TransitionFailure("failed to apply all the yaml files to the cluster", e);
            }
            this.logTransition(cmd);
        }

        @Override
        public void stopROIB(final TransitionCmd cmd) throws Issue {
            this.logTransition(cmd);
        }

        @Override
        public void stopDC(final TransitionCmd cmd) throws Issue {
            this.logTransition(cmd);
        }

        @Override
        public void stopHLT(final TransitionCmd cmd) throws Issue {
            try {
                deleteNewDeployment();
            } catch (ApiException e) {
                throw new ch.cern.k8sjava.KubeController.TransitionFailure("Failed to delete all the deployments", e);
            }
            this.logTransition(cmd);
        }

        @Override
        public void stopRecording(final TransitionCmd cmd) throws Issue {
            this.logTransition(cmd);
        }

        @Override
        public void stopGathering(final TransitionCmd cmd) throws Issue {
            this.logTransition(cmd);
        }

        @Override
        public void stopArchiving(final TransitionCmd cmd) throws Issue {
            this.logTransition(cmd);
        }

        @Override
        public void disconnect(final TransitionCmd cmd) throws Issue {
            this.logTransition(cmd);
        }

        @Override
        public void unconfigure(final TransitionCmd cmd) throws Issue {
            this.logTransition(cmd);
        }

        @Override
        public void subTransition(final SubTransitionCmd cmd) throws Issue {
            this.logTransition(cmd);
        }

        @Override
        public void resynch(final ResynchCmd cmd) throws Issue {
            this.logTransition(cmd);
        }

        private void logTransition(final TransitionCmd cmd) {
            ers.Logger.log("Executed transition " + cmd.getFSMCommand().name());
        }

        /**
         * Initializes all the K8S objects found in OKS and makes each of the Yaml file ready to be applied to the cluster.
         * @throws IOException Throws when it is unable to access one of the K8S Yaml configuration files
         */
        private void initKubernetsTypesFromOKS() throws IOException {
            final OnlineServices os = OnlineServices.instance();
            final dal.BaseApplication application = os.getApplication();

            /* Find the RunController */
            final KubernetesOperatorRCApplication ta = KubernetesOperatorRCApplication_Helper.cast(application);
            if (ta != null) {
                /* Get all the Kubernetes types that have a relation to this RunController */
                String yamlFilePath = ta.get_Yaml_file_path();
                ers.Logger.info("Init kubernetes yaml file: " + yamlFilePath);
            } else {
                throw new IOException("Unable to get the YAML file ")
            }
        }

        /**
         * Increments the RunControllerCustomResource's RunNumber value with +1.
         * That will cause the RunControllerOperator (program running inside the K8S Cluster) to register a new Run is
         * started and it needs to deploy a new deployment to handle the data from the new run.
         */
        private void startNewDeployment() {
            /**
             * updateRunControllerCustomResourceWithNewRun() does this:
             * 1. Get the CR
             * 2. Change the RunNumber to RunNumber+1
             * 3. Change the RunPipe to (whatever string) // not yet implemented
             * 4. Update the CR to the Cluster
             */
            RunControllerCustomResourceHelper customResourceHelper = new RunControllerCustomResourceHelper(this.kubernetesClient);
            customResourceHelper.updateRunControllerCustomResourceWithNewRun();
        }

        /**
         * Returns the kubeconfig file path as a String.
         * @return kubeconfig file path as a String
         * @throws IOException - Throws if it can not find the kubeconfig file in OKS
         */
        @NotNull
        private String getKubeconfigFilePathFromOKS() throws IOException {
            /* TODO: implement support for multiple kubeconfig files? This assumes only one kubeconfig file is used, even if you have multiple KubernetesRCApplications */
            final config.Configuration db = getDb();
            final dal.Partition dalPart = dal.Partition_Helper.get(db, partitionName);
            final dal.BaseApplication[] apps = dalPart.get_all_applications(new String[]{"KubernetesRCApplication"}, null, null); // TODO: support dynamic naming of the KubernetesRCApplication?

            for (final dal.BaseApplication a : apps) {
                final KubernetesRCApplication ta = KubernetesRCApplication_Helper.cast(a);
                if (ta != null) {
                    String kubeconfigFilePath = ta.get_kubeconfigFilePath();
                    if (kubeconfigFilePath != null && !kubeconfigFilePath.isEmpty()) {
                        return kubeconfigFilePath;
                    }
                }
            }
            IOException ex = new IOException("Can not find the kubeconfig file path in OKS");
            ers.Logger.warning(ex);
            throw ex;
        }


        private config.Configuration getDb() {
            if (this.db == null) {
                initDb();
            }
            return this.db;
        }

        /**
         * Call this function to set the default ApiClient that is used to talk to the Cluster
         * @param kubeconfigFilePath - Path to the kubeconfig file
         * @throws IOException - Throws if it can not read the kubeconfig file
         */
        private final void initK8SCluster(String kubeconfigFilePath) throws IOException {
            ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeconfigFilePath))).build();
            io.kubernetes.client.openapi.Configuration.setDefaultApiClient(client);
        }

        /**
         * The {@link config.Configuration} read-only and read-write objects are created. Subscription to database updates is done as well.
         * <p>
         * This method should always be called before {@link #setRootControllerData()} and {@link #initIS()}.
         *
         * @throws IguiException.ConfigException The {@link config.Configuration} object cannot be created or the database change subscription
         *             fails
         */
        private final void initDb() {
            // Do not initialize the DB if it already exists
            if (this.db == null) {
                try {
                    String dbConfigName = System.getenv("TDAQ_DB");
                    //config.Configuration db = new config.Configuration("rdbconfig:RDB");
                    this.db = new config.Configuration(dbConfigName);
                } catch (Exception e) {
                    System.err.println("ERROR caught \'config.SystemException\':");
                    System.err.println("*** " + e.getMessage() + " ***");
                    StackTraceElement[] stack = e.getStackTrace();
                    String errorMsg = "";
                    for (StackTraceElement s : stack) {
                        errorMsg += s.getFileName() + " " + s.getMethodName() + " " + s.getLineNumber() + "\n";
                    }
                    ers.Logger.info(errorMsg + e.getMessage() + " :: " + e.toString());
                }
            }
        }
    }

    public static void main(final String[] argv) {
        try {
            // Parse the command line options using the provided utility class
            final CommandLineParser cmdLine = new CommandLineParser(argv);
            partitionName = cmdLine.getPartitionName();

            // Create the JItemCtrl
            final JItemCtrl ic = new JItemCtrl(cmdLine.getApplicationName(),
                    cmdLine.getParentName(),
                    cmdLine.getSegmentName(),
                    //cmdLine.getPartitionName(),
                    partitionName,
                    new ch.cern.k8sjava.KubeController.MyControllable(),
                    cmdLine.isInteractive());

            // Initialize the run control framework
            ic.init();

            // This blocks: only now the application will be seen up for the process management
            ic.run();

            System.exit(0);
        }
        catch(final HelpRequested ex) {
            // The exception's message contains the help
            // Here some extra information may be printed (i.e., in case of additional specific command line arguments)
            System.out.println(ex.getMessage());

            System.exit(0);
        }
        catch(final ParseException | ItemCtrlException ex) {
            // Some error occurred
            ex.printStackTrace();

            System.exit(-1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-2);
        }
    }
}
