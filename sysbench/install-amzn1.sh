#!/bin/bash

# Install pre-requisites to execute scripts in this repo.
# This file for AMZN1 and Centos 6
# Note that this doesn't install MongoDB server, as it is expected to run on a separate host (in my world)

sudo yum -y groupinstall "Development tools"

# sysbench repo
curl -s https://packagecloud.io/install/repositories/akopytov/sysbench/script.rpm.sh | sudo bash

# EPEL repo (luarocks)
sudo sed -i "s/enabled=0/enabled=1/g" /etc/yum.repos.d/epel.repo
sudo yum -y install sysbench lua lua-devel luarocks
echo
echo
echo

# Sigh, libbson and libmongoc only available for EPEL 7 but I'm still on Centos 6:
curl -O --retry 10 -fsS http://dl.fedoraproject.org/pub/epel/7/x86_64/Packages/l/libbson-1.3.5-5.el7.x86_64.rpm
curl -O --retry 10 -fsS http://mirror.centos.org/centos/7/os/x86_64/Packages/libdb-5.3.21-24.el7.x86_64.rpm
curl -O --retry 10 -fsS http://mirror.centos.org/centos/7/os/x86_64/Packages/cyrus-sasl-lib-2.1.26-23.el7.x86_64.rpm
curl -O --retry 10 -fsS http://dl.fedoraproject.org/pub/epel/7/x86_64/Packages/m/mongo-c-driver-libs-1.3.6-1.el7.x86_64.rpm
curl -O --retry 10 -fsS http://dl.fedoraproject.org/pub/epel/7/x86_64/Packages/m/mongo-c-driver-1.3.6-1.el7.x86_64.rpm
curl -O --retry 10 -fsS http://dl.fedoraproject.org/pub/epel/7/x86_64/Packages/m/mongo-c-driver-devel-1.3.6-1.el7.x86_64.rpm
curl -O --retry 10 -fsS http://dl.fedoraproject.org/pub/epel/7/x86_64/Packages/l/libbson-devel-1.3.5-5.el7.x86_64.rpm
sudo rpm -i --nodeps libbson-1.3.5-5.el7.x86_64.rpm
sudo rpm -i --nodeps libdb-5.3.21-24.el7.x86_64.rpm
sudo rpm -i --nodeps --force cyrus-sasl-lib-2.1.26-23.el7.x86_64.rpm
sudo rpm -i --nodeps mongo-c-driver-libs-1.3.6-1.el7.x86_64.rpm
sudo rpm -i --nodeps mongo-c-driver-1.3.6-1.el7.x86_64.rpm
sudo rpm -i --nodeps libbson-devel-1.3.5-5.el7.x86_64.rpm
sudo rpm -i --nodeps mongo-c-driver-devel-1.3.6-1.el7.x86_64.rpm
sudo rm *.rpm
echo
echo

#luarocks install --local mongorover
rm -rf mongorover || true
git clone https://github.com/mongodb-labs/mongorover.git
cd mongorover
luarocks make --local mongorover*.rockspec
cd ..

luarocks install --local https://raw.githubusercontent.com/jiyinyiyong/json-lua/master/json-lua-0.1-3.rockspec
luarocks install --local penlight
