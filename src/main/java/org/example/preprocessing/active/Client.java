package org.example.preprocessing.active;

import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;

public class Client {
  public static void main(String[] args) throws Exception {
    // Load configuration from crail home config
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    CrailStore store = CrailStore.newInstance(conf);
    
    // Upload initial file(s)

    // Create Actions

    // Run workers

  }

  public static class Worker implements Runnable {

    @Override
    public void run() {
      // Read filtered data from Crail Action

      // Count words
      
    }

  }
}
