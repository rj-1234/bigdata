FROM ubuntu:18.04
MAINTAINER Rajeev Joshi <rj1234@nyu.edu>

RUN apt-get update
RUN apt-get install -y sudo
RUN apt-get update
RUN sudo yes | apt-get install software-properties-common

# Installing snap
RUN sudo yes | apt-get install snapd

# Sublime - doesn't work right now
#RUN sudo snap install sublime-text --classic

# Installing Java
RUN sudo apt-get update
RUN sudo apt-get install -y openjdk-8-jdk

# Installing Anaconda3 - source [http://docs.continuum.io/anaconda/install/silent-mode/]
RUN sudo yes | apt-get install wget
RUN sudo wget https://repo.anaconda.com/archive/Anaconda3-2019.03-Linux-x86_64.sh -O ~/anaconda.sh
RUN sudo bash ~/anaconda.sh -b -p ~/anaconda
RUN rm ~/anaconda.sh
#RUN echo 'export PATH="~/anaconda/bin:$PATH"' >> ~/.bash_profile 
ENV PATH="~/anaconda/bin:${PATH}"

# Installing Spark
RUN wget https://www-us.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz
RUN tar -xzf spark-2.4.3-bin-hadoop2.7.tgz && \
    mv spark-2.4.3-bin-hadoop2.7 ~/spark && \
    rm spark-2.4.3-bin-hadoop2.7.tgz

# Configure environment
ENV CONDA_HOME /root/anaconda
ENV SPARK_HOME /root/spark
ENV PATH $CONDA_HOME/bin:${PATH}
ENV PATH $SPARK_HOME/bin:${PATH}
RUN echo 
RUN echo ${PATH}
RUN echo 

# Environment vaiables for Spark to use Anaconda Python and jupyter notebook
ENV PYSPARK_PYTHON $CONDA_HOME/bin/python3
ENV PYSPARK_DRIVER_PYTHON $CONDA_HOME/bin/jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS "notebook \
								--no-browser \
								--port=8888 \
								--ip='0.0.0.0' \
								--allow-root"

# To allow container's jupyter notebook access on localhost's port -> 8888
EXPOSE 8888

WORKDIR ~/