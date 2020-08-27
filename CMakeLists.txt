tdaq_package()

tdaq_generate_dal(training.schema.xml
  PACKAGE k8sjava
  NAMESPACE daq::k8sjava
  INCLUDE_DIRECTORIES DFConfiguration RODBusyModule dal 
  INCLUDE k8sjava
  CPP_OUTPUT dal_cpp_srcs
  JAVA_OUTPUT dal_java_srcs)


#
# Build also a Java library in order to allow Java application to access the new OKS object
# Be aware of the included jars
#
tdaq_add_jar(jk8sjavadal DAL
  ${dal_java_srcs}
  INCLUDE_JARS dal/dal.jar config/config.jar DFConfiguration/DFdal.jar RODBusyModule/JRODBusydal.jar)

tdaq_add_jar_to_repo(jk8sjavadal.jar
    DESCRIPTION "jar for k8sjava DAL")


### Add everything needed for the Kubernetes RunController application

#### Add K8S test stuff
tdaq_add_jar(KubeController
  src/main/java/ch/cern/k8sjava/*.java
  src/main/java/ch/cern/k8sjava/util/*.java
  src/main/java/ch/cern/k8sjava/types/*.java
  INCLUDE_JARS k8sjava/jk8sjavadal.jar TM/testdal.jar ipc/ipc.jar Jers/ers.jar TDAQExtJars/external.jar config/config.jar dal/dal.jar)

tdaq_add_scripts(bin/kube_controller)

###
tdaq_add_to_repo(kube_controller
    SCRIPT
    DESCRIPTION "Kubernetes test application"
    SHELL "java"
    ENVIRONMENT TDAQ_JAVA_HOME)

tdaq_add_jar_to_repo(KubeController.jar
    DESCRIPTION "K8S Controller used to sending commands to a Kubernetes cluster")
