icore
=======================
-> This is the source for the Apache Tomcat Servlet, which is what we used for the API in our paper
-> Compiled with Redhat Developer Studio (latest version as of 12/3/2018)
-> Spark/CQL/DataStax dependencies are in the (typical Java server) WEB-INF directory, these .jars (library archives) were copied from the spark install directory on each of the nodes (/home/mona/spark-...)

How to Setup Dev Env and Compile/Deploy Tomcat (icore) Server
=============================================================
-> Install Java 8: Current version on nodes: 
	- java version "1.8.0_171"
	- Java(TM) SE Runtime Environment (build 1.8.0_171-b11)
	- Java HotSpot(TM) 64-Bit Server VM (build 25.171-b11, mixed mode)
-> Install Apache Tomcat (https://www.digitalocean.com/community/tutorials/how-to-install-apache-tomcat-8-on-ubuntu-16-04)
	-> My installed version is 8.5.35; find version: https://archive.apache.org/dist/tomcat/tomcat-8/ 
	-> You can put the Tomcat directory wherever you'd like
-> Install Redhat Developer Studio (RDS) (https://developers.redhat.com/products/codeready-studio/download)
	-> My installed version is 12.9.0.GA 
	-> You may have to point RDS at your Java installation location
	-> https://developers.redhat.com/products/devstudio/download/
	-> Red Hat comes with a Wildfly server (this is the new version of JBOSS, one of the most popular Java web servers)
-> Configure Tomcat in RDS
	-> Window -> Preferences -> Server -> Runtime Environment -> Add ... (should be straightforward from here)
-> Import the Eclipse project under <your_git_clone_dir>/icore/project/icore
	-> File -> Import -> General -> Existing Projects into Workspace
	-> Add the project to your local Tomcat server
		-> Make sure Servers view is open, -> Window -> Show View -> Server
		-> In Servers window, right  click Tomcat v8.5 server and then Add and Remove, add the icore project (make sure it's on the right)
	-> If you get compiler errors, right click the project in Package Explorer, go to Java Build Path, and make sure you have the following entries
		-> Apache Tomcat v8.5 (this should have been added when you added Tomcat to RDS)
		-> EAR Libraries (this should be bundled with your RDS)
		-> JRE System Library [JaveSE-1.7] (this should point to your Java 8 installer, yes for some reason JavaSE-1.7 is called Java 8... idk why)
		-> Web App Libraries (this should point to <your_git_clone_dir>/icore/project/icore/WebContent/WEB-INF)
-> Project should now be compiled and deployed locally automatically, but...
	-> You can right click the Tomcat server under the servers view to Publish (CTRL+ALT+P) or start/restart (CTRL+ALT+R)
	-> Publishing copies the compiled JARs into your Tomcat server directory and writes the necessary config
	-> Don't bother starting the local Tomcat, because Spark is not installed on your dev env, it will throw errors while trying to connect when it boots 
-> To deploy...
	-> After publishing locally, simply copy the entire icore directory in your Tomcat server (this is in <apache_root_dir>/webapps/icore) to the same directory on the driver node
	-> Copying WEB-INF takes some time.  To skip this step, only copy ../icore/WEB-INF/classes.  If new classes were added (or config changed), also copy icore/META-INF.
	-> Run a `sudo sysctl restart tomcat` on the driver node
	-> Logs can be viewed by running `tail` on /opt/tomcat/logs/catalina.out
	-> The iCoRe cache can be cleared by doing an `rm *` in /mnt/apicache/
