#ifndef __NETWORK_CONFIG_H__
#define __NETWORK_CONFIG_H__

#include <string>
#include "common.h"

/*
 * Network based configuration and directory service
 */

class network_config {
 public:
  // gets a vector of machine/port pairs for a named service
  // returns true on success
  static bool getService(const std::string& serviceName,
                         const std::string& options,
                         server_vector_t& _return) {
    return false;
  }
};

#define USE_ZOOKEEPER_SMC
#ifdef USE_ZOOKEEPER_SMC

#include "zookeeper.h"

class ZooKeeperConfig : public network_config {
 public:
  ZooKeeperConfig(const std::string& servers);
  ~ZooKeeperConfig();
  bool getService(const std::string& serviceName,
                  const std::string& options,
                  server_vector_t& _return);
 protected:
  zhandle_t *zkh;  
 private:
  bool close();
};

#endif

#endif
