# This is a version of MXNET with DGT functionality.

Differential Gradient Transmission (DGT) is a  contribution-aware differential gradient transmission mechanism for distributed machine learning. The basic idea of DGT is to provide differential transmission service for gradients according to their contribution to model convergence.
## Setup
The installation of current version can refer to the MXNET's installation instruction (https://mxnet.apache.org/get_started/ubuntu_setup ) from source code.

## Configurable Environment variable


| Name      |     Description |
| :-------- | --------|
| ENABLE_DGT| The variable controls whether the DGT function is enabled. ENABLE_DGT=0 indicates that the DGT function is not enabled and the default gradient transmission of MXNET is used. ENABLED_DGT=1 indicates that the DGT function is enabled.  |
| DMLC_UDP_CHANNEL_NUM | The variable sets the number of unreliable channels and supports value from 1 to 7. |
|DMLC_K| The variable sets the initial classification threshold p<sub>0</sub>, and supports value ranging from 0.0 to 1.0.  |
|ADAPTIVE_K_FLAG| The variable controls whether the function of convergence-aware update method for classification threshold is enabled. ADAPTIVE_K_FLAG = 1 enables the function.|
|SUDO_PASSWD|The variable is the SUDO password provided by the user. This environment variable is read during the initialization of the transmission channels and is used to support the priority setteing of channels| 
|DGT_ENABLE_BLOCK|The variable controls whether the function of gradient partition is enabled. DGT_ENABLE_BLOCK=1 enables the function. This variable is used for testing|
|DGT_BLOCK_SIZE|The variable sets the block size of gradient partition predefined by user|
|DGT_RECONSTRUCT|The variable is used for testing, DGT_RECONSTRUCT=1 indicates that Diff-Reception method is adopted at receving end and DGT_RECONSTRUCT=0 indicates that "Heuristic Dropping" method is adopted.|
|DGT_CONTRI_ALPHA|The variable sets the contribution momentum factor and support value from 0.0 to 1.0.|
|DGT_ENABLE_SEND_DROP|The variable is used for testing. DGT_ENABLE_SEND_DROP=1 indicates that Sender Dropping (SD) solution is adopted for gradient transmission|
|DGT_SET_RANDOM| The variable is used for testing. DGT_SET_RANDOM = 1 indicates that gradients are sorted randomly before classification which is used for verifying the effect of contribution-aware differential transmission.|
|DGT_INFO|The variable controls whether to display the debugging information of the DGT component for testing. |
## Deployment Example
Here we give a deployment example for current version. Except the default start script of MXNET, we add some variable settings for DGT function.  If the cluster has 1 parameter server and 2 worker, we deploy the distributed learning with following start script. 

    #Scheduler
    export LD_LIBRARY_PATH=:/home/homan/incubator-mxnet/deps/lib:/usr/local/cuda-9.0/lib64; export DMLC_NUM_WORKER=2; export DMLC_NUM_SERVER=1; export DMLC_PS_ROOT_URI=10.2.1.11; export DMLC_PS_ROOT_PORT=9120; export DMLC_ROLE=scheduler; export DMLC_NODE_HOST=10.2.1.11; python xxxx.py
    #Server
    export LD_LIBRARY_PATH=:/home/homan/incubator-mxnet/deps/lib:/usr/local/cuda-9.0/lib64; export DMLC_NUM_WORKER=2; export DMLC_NUM_SERVER=1; export DMLC_PS_ROOT_URI=10.2.1.11; export DMLC_PS_ROOT_PORT=9120; export DMLC_UDP_CHANNEL_NUM=7; export DMLC_K=0.8; export ADAPTIVE_K_FLAG=1; export ENABLE_DGT=1;export DMLC_ROLE=server; export DMLC_NODE_HOST=10.2.1.11; export PS_VERBOSE=1; export SUDO_PASSWD=111111 ; export DGT_INFO=1; export DGT_RECONSTRUCT=1; python xxxx.py
    #Worker 1
    export LD_LIBRARY_PATH=:/home/homan/incubator-mxnet/deps/lib:/usr/local/cuda-9.0/lib64; export DMLC_NUM_WORKER=2; export DMLC_NUM_SERVER=1; export DMLC_PS_ROOT_URI=10.2.1.11; export DMLC_PS_ROOT_PORT=9120; export DMLC_UDP_CHANNEL_NUM=7; export DMLC_K=0.8; export ADAPTIVE_K_FLAG=1; export ENABLE_DGT=1;export DMLC_ROLE=worker; export DMLC_NODE_HOST=10.2.1.10; export PS_VERBOSE=1; export SUDO_PASSWD=111111 ; export DGT_ENABLE_SEND_DROP=0;DGT_CONTRI_ALPHA=0.3; export DGT_SET_RANDOM=0; export DGT_INFO=1;export DGT_ENABLE_BLOCK=1;export DGT_BLOCK_SIZE=4096; export DGT_RECONSTRUCT=1; python xxxx.py
    #Worker 2
     export LD_LIBRARY_PATH=:/home/homan/incubator-mxnet/deps/lib:/usr/local/cuda-9.0/lib64; export DMLC_NUM_WORKER=2; export DMLC_NUM_SERVER=1; export DMLC_PS_ROOT_URI=10.2.1.11; export DMLC_PS_ROOT_PORT=9120; export DMLC_UDP_CHANNEL_NUM=7; export DMLC_K=0.8; export ADAPTIVE_K_FLAG=1; export ENABLE_DGT=1;export DMLC_ROLE=worker; export DMLC_NODE_HOST=10.2.1.12; export PS_VERBOSE=1; export SUDO_PASSWD=111111 ; export DGT_ENABLE_SEND_DROP=0;DGT_CONTRI_ALPHA=0.3; export DGT_SET_RANDOM=0; export DGT_INFO=1;export DGT_ENABLE_BLOCK=1;export DGT_BLOCK_SIZE=4096; export DGT_RECONSTRUCT=1; python xxxx.py



