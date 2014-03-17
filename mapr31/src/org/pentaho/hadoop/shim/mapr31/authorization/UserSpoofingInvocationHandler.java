package org.pentaho.hadoop.shim.mapr31.authorization;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.security.UserGroupInformation;

public class UserSpoofingInvocationHandler<T> implements InvocationHandler {
  private final T delegate;
  private final Set<Class<?>> interfacesToDelegate;
  private final String user;

  public UserSpoofingInvocationHandler( T delegate ) {
    this( delegate, new HashSet<Class<?>>(), null );
  }

  public UserSpoofingInvocationHandler( T delegate, Set<Class<?>> interfacesToDelegate ) {
    this( delegate, interfacesToDelegate, null );
  }
  
  public UserSpoofingInvocationHandler( T delegate, Set<Class<?>> interfacesToDelegate, String user ) {
    this.delegate = delegate;
    this.interfacesToDelegate = interfacesToDelegate;
    this.user = user;
  }

  public static <T> T forObject( T delegate, Set<Class<?>> interfacesToDelegate ) {
    return forObject( delegate, interfacesToDelegate, null );
  }
  
  @SuppressWarnings( "unchecked" )
  public static <T> T forObject( T delegate, Set<Class<?>> interfacesToDelegate, String user ) {
    return (T) Proxy.newProxyInstance( delegate.getClass().getClassLoader(), (Class<?>[]) ClassUtils.getAllInterfaces(
        delegate.getClass() ).toArray( new Class<?>[] {} ), new UserSpoofingInvocationHandler<Object>( delegate,
        interfacesToDelegate, user ) );
  }
  
  private Method getMethodForUserName(Method originalMethod) {
    try {
      return delegate.getClass().getMethod( originalMethod.getName() + "GetUser", originalMethod.getParameterTypes() );
    } catch ( Exception e ) {
      // noop;
    }
    return null;
  }

  @Override
  public Object invoke( Object proxy, final Method method, final Object[] args ) throws Throwable {
    try {
      UserGroupInformation userToImpersonate = UserGroupInformation.getLoginUser();
      PrivilegedExceptionAction<Object> action = new PrivilegedExceptionAction<Object>() {
        
        @Override
        public Object run() throws Exception {
          Object result = method.invoke( delegate, args );
          if ( result != null ) {
            for ( Class<?> iface : result.getClass().getInterfaces() ) {
              if ( interfacesToDelegate.contains( iface ) ) {
                result = forObject( result, interfacesToDelegate, UserGroupInformation.getCurrentUser().getUserName() );
                break;
              }
            }
          }
          return result;
        }
      };

      if ( user == null ) {
        Method methodForUsername = getMethodForUserName( method );
        if ( methodForUsername != null ) {
          String username = (String) methodForUsername.invoke( delegate, args );
          if ( username != null && username.length() > 0 && !username.equals( userToImpersonate.getUserName() ) ) {
            userToImpersonate = UserGroupInformation.createProxyUser( username, userToImpersonate );
          }
        }
      } else if ( !user.equals( userToImpersonate.getUserName() ) ) {
        userToImpersonate = UserGroupInformation.createProxyUser( user, userToImpersonate );
      }
      
      return userToImpersonate.doAs( action );
    } catch ( Exception e ) {
      if ( e.getCause() instanceof InvocationTargetException ) {
        throw ( (InvocationTargetException) e.getCause() ).getCause();
      }
      throw e;
    }
  }
}
