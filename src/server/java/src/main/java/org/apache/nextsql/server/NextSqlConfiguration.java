package org.apache.nextsql.server;

import org.apache.nextsql.conf.Configuration;

public class NextSqlConfiguration extends Configuration {
  static {
    addDeprecatedKeys();
    // adds the default resources
    Configuration.addDefaultResource("ns-default.xml");
    Configuration.addDefaultResource("ns-site.xml");
  }
  
  public NextSqlConfiguration() {
    super();
  }
  
  public NextSqlConfiguration(boolean aLoadDefaults) {
    super(aLoadDefaults);
  }
  
  public NextSqlConfiguration(Configuration aConf) {
    super(aConf);
  }
  
  private static void addDeprecatedKeys() {
      // add deprecated keys
  }
}