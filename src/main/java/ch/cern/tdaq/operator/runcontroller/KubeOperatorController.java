package ch.cern.tdaq.operator.runcontroller;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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

import tdaqoperator.KubernetesTdaqOperatorRCApplication;
import tdaqoperator.KubernetesTdaqOperatorRCApplication_Helper;

import config.Configuration;

public class KubeOperatorController {
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
        private config.Configuration db = null;

        MyControllable() throws Exception {
            super();
        }

        @Override
        public void publish() throws Issue {
            ers.Logger.log("Publishing..."); //ers.Logger.info() .warning()
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
                /* Parse user command, find "newrun" and if so -> start a new Deployment. Dirty and quick */
                if (usrCmdParams != null) {
                    for (int i = 0; i < usrCmdParams.length; i++) {
                        String input = usrCmdParams[i].toLowerCase();
                        if (input.contains("newrun")) {
                            startNewDeployment();
                            break;
                        }
                    }
                }
            } catch (final Exception e) {
                ers.Logger.warning(e);
                throw new ch.cern.tdaq.operator.runcontroller.KubeOperatorController.UserCmdFailure("Failed to complete the user command " + commandName + ". " + e.getMessage(), e);
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
                initDb();
                String kubeConfigPath = getKubeconfigFilePathFromOKS();
                initK8SCluster(kubeConfigPath);
            }
            catch(final OnlineServicesFailure | IOException ex) {
                throw new ch.cern.tdaq.operator.runcontroller.KubeOperatorController.TransitionFailure("Cannot retrieve configuration information: " + ex.getMessage(), ex);
            }
            this.logTransition(cmd);
        }

        @Override
        public void connect(final TransitionCmd cmd) throws Issue {
            this.logTransition(cmd);
        }

        @Override
        public void prepareForRun(final TransitionCmd cmd) throws Issue {
            final rc.RunParamsNamed isInfo = new rc.RunParamsNamed(Igui.instance().getPartition(),
                    IguiConstants.RUNPARAMS_IS_INFO_NAME,
                    rc.RunParamsNamed.type.getName());

            try {
                isInfo.checkout();
            }
            catch(final is.InfoNotFoundException ex) {
                // Can be ignored
            }
            catch(final Exception ex) {
                throw new IguiException.ISException("Checkout from RunParams IS server failed: " + ex, ex);
            }

            long runNumber = isInfo.run_number;
            final String runType = runParamsInfo.run_type;
            String beamType = runParamsInfo.beam_type;
            String partitionName = IguiConstants.TDAQ_INITIAL_PARTITION;

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
         * Increments the RunControllerCustomResource's RunNumber value with +1.
         * That will cause the RunControllerOperator (program running inside the K8S Cluster) to register a new Run is
         * started and it needs to deploy a new deployment to handle the data from the new run.
         */
        private void startNewDeployment() throws IOException {
            /**
             * updateRunControllerCustomResourceWithNewRun() does this:
             * 1. Get the CR
             * 2. Change the RunNumber to RunNumber+1
             * 3. Change the RunPipe to (whatever string) // not yet implemented
             * 4. Update the CR to the Cluster
             */
            try (final KubernetesClient kubernetesClient = new DefaultKubernetesClient()) {
                RunControllerCustomResourceHelper customResourceHelper = new RunControllerCustomResourceHelper(kubernetesClient);
                customResourceHelper.updateRunControllerCustomResourceWithNewRun();
            }
        }

        /**
         * Returns the kubeconfig file path as a String.
         * @return kubeconfig file path as a String
         * @throws IOException - Throws if it can not find the kubeconfig file in OKS
         */
        @NotNull
        private String getKubeconfigFilePathFromOKS() throws IOException, OnlineServicesFailure {
            final OnlineServices os = OnlineServices.instance();
            final dal.RunControlApplicationBase application = os.getApplication();

            final KubernetesTdaqOperatorRCApplication app = KubernetesTdaqOperatorRCApplication_Helper.cast(application);
            if (app != null) {
                String kubeconfigFilePath = app.get_kubeconfigFilePath();
                if (kubeconfigFilePath != null && !kubeconfigFilePath.isEmpty()) {
                    return kubeconfigFilePath;
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
         * Call this function to set the path to the default kubeconfig file, that is used to connect to the cluster
         * @param kubeconfigFilePath - Path to the kubeconfig file
         */
        private final void initK8SCluster(String kubeconfigFilePath) {
            /* Fabric8 K8S Library uses the Java Properties (or as a falback the enviroment variable) to find the kubeconfig file */
            System.setProperty("kubeconfig", kubeconfigFilePath);
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
                    ers.Logger.info(e.getMessage() + " :: " + e.toString());
                }
            }
        }
    }

    public static void main(final String[] argv) {
        try {
            // Parse the command line options using the provided utility class
            final CommandLineParser cmdLine = new CommandLineParser(argv);
            String partitionName = cmdLine.getPartitionName();

            // Create the JItemCtrl
            final JItemCtrl ic = new JItemCtrl(cmdLine.getApplicationName(),
                    cmdLine.getParentName(),
                    cmdLine.getSegmentName(),
                    partitionName,
                    new KubeOperatorController.MyControllable(),
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
