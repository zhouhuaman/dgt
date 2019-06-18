#!/bin/bash
echo "USE_BLAS=openblas" >> ./make/config.mk
echo "USE_OPENCV=1" >> ./make/config.mk
echo "USE_CUDA=1" >> ./make/config.mk
echo "USE_CUDA_PATH = /usr/local/cuda" >> ./make/config.mk
echo "USE_CUDNN=1" >> ./make/config.mk
echo "USE_DIST_KVSTORE=1" >> ./make/config.mk
make -j $(nproc)
cd ./lib
scp *.so* homan@10.1.1.29:/home/homan/mxnet_test_src/resnet_mnist/mxnet/
scp *.so* homan@10.1.1.33:/home/homan/mxnet_test_src/resnet_mnist/mxnet/
#scp *.so*  dcn@10.2.1.15:/home/dcn/mxnet_test_src/resnet_mnist/mxnet/

