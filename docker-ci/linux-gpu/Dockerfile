FROM nvidia/cuda:10.0-devel-ubuntu18.04
COPY scripts /tmp/scripts
RUN /tmp/scripts/apt-install.sh && rm -f /tmp/scripts/apt-install.sh
# Make sure CUDA libraries are accessible at runtime
RUN echo "/usr/local/cuda-10.0/lib64/stubs" >> /etc/ld.so.conf.d/cuda-10-0.conf
RUN ldconfig
# Set Python up after configuring CUDA properly, in case Python stuff needs to be built with CUDA support
RUN /tmp/scripts/setup-python.sh && rm -f /tmp/scripts/setup-python.sh
