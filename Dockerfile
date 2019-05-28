FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install -y sudo
RUN apt-get update
RUN sudo yes | apt-get install software-properties-common

# python 3
RUN sudo apt-get install -y g++ python3.6 python3-pip

# python 2
RUN apt-get install -y python2.7
RUN ln -s /usr/bin/python2.7 /usr/bin/python2

# default is python 3
RUN ln -s /usr/bin/python3.6 /usr/bin/python

# snap
RUN sudo yes | apt-get install snapd

# Sublime
#RUN sudo snap install sublime-text --classic

# Java
RUN sudo apt-get update
RUN sudo apt-get install -y openjdk-8-jdk

# Anaconda3 - source [http://docs.continuum.io/anaconda/install/silent-mode/]
RUN sudo yes | apt-get install wget
RUN sudo wget https://repo.anaconda.com/archive/Anaconda3-2019.03-Linux-x86_64.sh -O ~/anaconda.sh
RUN sudo bash ~/anaconda.sh -b -p ~/anaconda
RUN rm ~/anaconda.sh
#RUN echo 'export PATH="~/anaconda/bin:$PATH"' >> ~/.bash_profile 
ENV PATH="~/anaconda/bin:${PATH}"

# Spark
RUN wget https://www-us.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz
RUN tar -xzf spark-2.4.3-bin-hadoop2.7.tgz && \
    mv spark-2.4.3-bin-hadoop2.7 ~/spark && \
    rm spark-2.4.3-bin-hadoop2.7.tgz

# Configure environment
ENV CONDA_DIR /root/anaconda
ENV SPARK_DIR /root/spark
ENV PATH $CONDA_DIR/bin:${PATH}
ENV PATH $SPARK_DIR/bin:${PATH}
RUN echo ${PATH}
#ENV PATH="~/spark/bin:${PATH}"

#Environment vaiables for Spark to use Anaconda Python and iPython notebook
ENV PYSPARK_PYTHON $CONDA_DIR/bin/python3
ENV PYSPARK_DRIVER_PYTHON $CONDA_DIR/bin/jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS "notebook --no-browser --port=8888 --ip='0.0.0.0' --allow-root"

EXPOSE 8888
EXPOSE 4040

WORKDIR ~/