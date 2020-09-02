tdaq_package()

tdaq_generate_dal(training.schema.xml
  PACKAGE tdaqoperator
  NAMESPACE daq::tdaqoperator
  INCLUDE_DIRECTORIES DFConfiguration RODBusyModule dal 
  INCLUDE tdaqoperator
  CPP_OUTPUT dal_cpp_srcs
  JAVA_OUTPUT dal_java_srcs)


#
# Build also a Java library in order to allow Java application to access the new OKS object
# Be aware of the included jars
#
tdaq_add_jar(tdaqoperatordal DAL
  ${dal_java_srcs}
  INCLUDE_JARS dal/dal.jar config/config.jar DFConfiguration/DFdal.jar RODBusyModule/JRODBusydal.jar)

tdaq_add_jar_to_repo(tdaqoperatordal.jar
    DESCRIPTION "jar for tdaq K8S operator Run Controller DAL")


### Add everything needed for the Kubernetes TDAQ Operator RunController application

#### Add K8S test stuff
tdaq_add_jar(KubeOperatorController
  src/main/java/ch/cern/tdaq/operator/runcontroller/*.java
  src/main/java/ch/cern/tdaq/operator/runcontroller/CustomResource/*.java
  INCLUDE_JARS tdaq-operator-run-controller/tdaqoperatordal.jar TM/testdal.jar ipc/ipc.jar Jers/ers.jar TDAQExtJars/external.jar config/config.jar dal/dal.jar)

tdaq_add_scripts(bin/kube_operator_controller)

###
tdaq_add_to_repo(kube_operator_controller
    SCRIPT
    DESCRIPTION "Kubernetes TDAQ Operator controller"
    SHELL "java"
    ENVIRONMENT TDAQ_JAVA_HOME)

tdaq_add_jar_to_repo(KubeOperatorController.jar
    DESCRIPTION "K8S Controller used to sending commands to the Kubernetes TDAQ Operator")
