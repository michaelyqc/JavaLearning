package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        try {
            Configuration conf = new Configuration();
            conf.addResource(new Path("/usr/ndp/current/hdfs_client/conf/core-site.xml")); 
            conf.addResource(new Path("/usr/ndp/current/hdfs_client/conf/hdfs-site.xml")); 
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.set("hadoop.security.authentication", "kerberos");

            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(
                "impala/hzadg-ambari-dev9.server.163.org@BDMS.163.COM",
                "/etc/security/keytabs/impala.service.keytab"
            );
           
            /*
            final FileSystem fs = FileSystem.get(conf);
            // system/service user able to proxy
            UserGroupInformation proxyUser = UserGroupInformation.getCurrentUser();
            // user = user to impersonate
            UserGroupInformation ugi = UserGroupInformation.createProxyUser("test", proxyUser);
            ugi.doAs( new PrivilegedExceptionAction<Void>() {
                          public Void run() throws Exception {
                              //OR access hdfs
                              fs.mkdirs( new Path("/user/test") );
                              return null;
                          }
            });
             */
           
            FileSystem hdfs = FileSystem.get(conf);

            Path rootDir = new Path("/user/impala");
            FileStatus files[] = hdfs.listStatus(rootDir);
            for(FileStatus file:files) {
                System.out.println(file.getPath());
            }

            hdfs.delete(new Path("/user/impala/hdfs-site.xml"));
            hdfs.delete(new Path("/user/impala/.Trash/Current/user/impala/hdfs-site.xml"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println( "Hello World!" );
    }
}
