#!/bin/bash
echo "USE_BLAS=openblas" >> ./make/config.mk
echo "USE_OPENCV=1" >> ./make/config.mk
echo "USE_CUDA=1" >> ./make/config.mk
echo "USE_CUDA_PATH = /usr/local/cuda" >> ./make/config.mk
echo "USE_CUDNN=1" >> ./make/config.mk
echo "USE_DIST_KVSTORE=1" >> ./make/config.mk
make -j $(nproc)
aaaaaaaaaa
