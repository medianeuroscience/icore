#Script to extend data storage for /var/lib/cassandra

mkfs.ext4 /dev/vdd

mkdir /ext

mount /dev/vdd /ext

cp -r /var/lib/cassandra/ /ext

rm -r /ext/lost+found

umount /dev/vdd

mount /dev/vdd /var/lib/cassandra

chown -R cassandra:cassandra /var/lib/cassandra/






