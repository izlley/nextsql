package org.apache.nextsql.multipaxos;

import org.apache.nextsql.conf.Configuration;

public class PaxosConfiguration extends Configuration {
  static {
    addDeprecatedKeys();
    // adds the default resources
    Configuration.addDefaultResource("paxos-default.xml");
    Configuration.addDefaultResource("paxos-site.xml");
  }
  
  public PaxosConfiguration() {
    super();
  }
  
  public PaxosConfiguration(boolean aLoadDefaults) {
    super(aLoadDefaults);
  }
  
  public PaxosConfiguration(Configuration aConf) {
    super(aConf);
  }
  
  private static void addDeprecatedKeys() {
      // add deprecated keys
  }
}