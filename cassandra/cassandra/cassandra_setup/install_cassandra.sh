#Installation Script for Cassandra
#Ubuntu18
#Frederic Hopp / Media Neuroscience Lab 2018

sudo add-apt-repository -y ppa:webupd8team/java

sudo apt-get update

#sudo apt-get -y install oracle-java8-installer

echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list

curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -

sudo apt-get update

sudo apt-key adv --keyserver pool.sks-keyservers.net --recv-key A278B781FE4B2BDA

sudo apt-get install cassandra

#sudo service cassandra start

#sudo apt install python-pip

#sudo pip install cassandra-driver
