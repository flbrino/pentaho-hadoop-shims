package org.pentaho.hadoop.shim.mapr31.authorization;

import java.util.HashSet;

import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

public class UserSpoofingHadoopAuthorizationService extends NoOpHadoopAuthorizationService implements HadoopAuthorizationService {
  protected static final String HDFS_PROXY_USER = "pentaho.hdfs.proxy.user";
  protected static final String MR_PROXY_USER = "pentaho.mapreduce.proxy.user";

  @Override
  public HadoopShim getHadoopShim() {
    return UserSpoofingInvocationHandler.forObject( new org.pentaho.hadoop.shim.mapr31.HadoopShim() {
      
      @SuppressWarnings( "unused" )
      public String getFileSystemGetUser( Configuration conf ) {
        return conf.get( HDFS_PROXY_USER );
      }
      
      @SuppressWarnings( "unused" )
      public String submitJobGetUser( Configuration c ) {
        return c.get( MR_PROXY_USER );
      }
    }, new HashSet<Class<?>>() );
  }
}
