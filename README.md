# Crail-ReFlex

[ReFlex](https://github.com/stanford-mast/reflex) storage backend for [Crail](https://github.com/zrlio/crail).


## Building Crail-ReFlex

Building the source requires [Apache Maven](http://maven.apache.org/) and [GNU/autotools](http://www.gnu.org/software/autoconf/autoconf.html) and Java version 8 or higher.
To build Crail-ReFlex and its example programs, execute the following steps:

1. Compile Crail, see instructions in [Crail README](https://github.com/zrlio/crail). You will first need to compile [DiSNI](https://github.com/zrlio/disni) and [DARPC](https://github.com/zrlio/darpc).

2. Compile the Java sources for Crail-ReFlex and copy jar to Crail home directory: 

   ```
   mvn -DskipTests install
   cp /path/to/crail-reflex/target/crail-reflex-1.0.jar /path/to/crail/assembly/target/crail-1.0/bin/jars/
   ```

3. Compile libreflex using: 
   
   ```
   cd libreflex 
   ./autoprepare.sh
   ./configure --with-jdk=\<path\>
   sudo make install
   ```

4. Make sure libreflex is in your library path: 

   ```
   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
   ```

A Crail-ReFlex datanode communicates with a Crail namenode. There are several options for the namenode setup. If your hardware supports RDMA, you can use the default DARPC-based namenode in Crail. Otherwise, you can either i) setup [SoftiWARP](https://github.com/zrlio/softiwarp) for software-based RDMA support and run the default DARPC Crail namenode or ii) run the [Crail-netty](https://github.com/zrlio/crail-netty) namenode which uses TCP/IP and does not require RDMA support.


## Running a simple example

To run a simple benchmark that connects to a ReFlex server and sends I/O requests, run the following command:

   ```
   java com.ibm.disni.benchmarks.ReFlexEndpointClient -a <IP_ADDR> -p <PORT> -m <RANDOM,SEQUENTIAL> -i <NUM_ITER> -rw <READ_FRACTION> -s <REQ_SIZE_BYTES> -qd <QUEUE_DEPTH>
   ```

Set the classpath (with `-cp` option) to
`/usr/local/lib:/path/to/crail/crail/assembly/target/crail-1.0-bin/jars/*:/path/to/crail-reflex/target/*`. 


## Running a Crail-ReFlex datanode

Set the namenode IP address and ReFlex storage tier properties in `crail-site.conf` and `core-site.xml`. 
Example `crail-site.conf` ReFlex storage tier property settings:

   ```
   crail.storage.types                     com.ibm.crail.storage.reflex.ReFlexStorageTier
   crail.storage.reflex.bindip             10.79.6.130
   crail.storage.reflex.port               1234
   ```

Start the crail-reflex datanode with the following command:

   ```
   ./bin/crail datanode -t com.ibm.crail.storage.reflex.ReFlexStorageTier 
   ```
