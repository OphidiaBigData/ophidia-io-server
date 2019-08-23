FROM centos:7

USER root

EXPOSE 65000

RUN cd / && \
	yum -y update && \
	yum -y install epel-release && \
	yum -y install sed coreutils && \
	yum -y install gcc make flex compat-guile18 compat-guile18-devel flex-devel guile-devel guile bison curl iproute && \
	yum -y install hdf5-devel hdf5-mpich-devel && \
	yum -y install netcdf-devel netcdf-mpich-devel && \
	yum -y install http://repo.mysql.com/mysql57-community-release-el7.rpm && \
	yum -y install mysql-community-client && \
	yum -y clean all

RUN mkdir -p /repo && \
	cd /repo && \
	curl -O http://ftp.gnu.org/gnu/libmatheval/libmatheval-1.1.11.tar.gz && \
	curl -O -k https://download.ophidia.cmcc.it/rpm/1.5/ophidia-primitives-1.5.0-0.el7.centos.x86_64.rpm && \
	curl -O -k https://download.ophidia.cmcc.it/rpm/1.5/ophidia-io-server-1.5.0-0.el7.centos.x86_64.rpm

# install libreries
RUN cd /repo && \
	tar -xzf libmatheval-1.1.11.tar.gz && \
	cd /repo/libmatheval-1.1.11 && \
	./configure --prefix=/usr/local/ophidia/extra && \
	make && \
	make install

# install the server
RUN cd /repo && \
	yum -y install ophidia-primitives*.x86_64.rpm && \
	yum -y install ophidia-io-server*.x86_64.rpm

# change ip address of the container
RUN cd /usr/local/ophidia/oph-cluster/oph-io-server/etc && \
	sed -i "s/127.0.0.1/0.0.0.0/g" oph_ioserver.conf && \
	useradd -r ophidia -d /usr/local/ophidia && \
	chown -R ophidia:ophidia /usr/local/ophidia

# Clean up
RUN cd / && rm -rf /repo

COPY entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]
CMD ["abcd" "172.17.0.1" "3306" "192.168.0.1"]

