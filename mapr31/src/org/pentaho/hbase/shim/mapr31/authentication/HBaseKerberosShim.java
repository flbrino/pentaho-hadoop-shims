package org.pentaho.hbase.shim.mapr31.authentication;

import java.util.Arrays;
import java.util.HashSet;

import javax.security.auth.login.LoginContext;

import org.pentaho.hadoop.shim.mapr31.authorization.KerberosInvocationHandler;
import org.pentaho.hadoop.shim.mapr31.delegatingShims.DelegatingHBaseConnection;
import org.pentaho.hbase.shim.mapr31.MapRHBaseConnection;
import org.pentaho.hbase.shim.mapr31.MapRHBaseShim;
import org.pentaho.hbase.shim.mapr31.wrapper.HBaseConnectionInterface;
import org.pentaho.hbase.shim.mapr31.wrapper.HBaseShimInterface;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.pentaho.hbase.shim.spi.HBaseConnection;

public class HBaseKerberosShim extends MapRHBaseShim implements HBaseShimInterface {
  private final LoginContext loginContext;

  public HBaseKerberosShim( LoginContext loginContext ) {
    this.loginContext = loginContext;
  }

  @Override
  public HBaseConnection getHBaseConnection() {
    return new DelegatingHBaseConnection( KerberosInvocationHandler.forObject( loginContext, new MapRHBaseConnection(),
        new HashSet<Class<?>>( Arrays.<Class<?>> asList( HBaseShimInterface.class, HBaseConnectionInterface.class,
            HBaseBytesUtilShim.class ) ) ) );
  }
}
