#define USE_ZOOKEEPER_SMC
#ifdef USE_ZOOKEEPER_SMC

#include <string>

#include "zookeeper.h"
#include "network_config.h"

/*
ZOOAPI zhandle_t *zookeeper_init(const char *host, watcher_fn fn,
  int recv_timeout, const clientid_t *clientid, void *context, int flags);
*/

ZooKeeperConfig::ZooKeeperConfig(const std::string& servers) {

  zkh = zookeeper_init(servers.c_str(), NULL, 0, NULL, NULL, 0);
  zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
  zoo_deterministic_conn_order(0);

}

bool ZooKeeperConfig::close() {
  zookeeper_close(zkh);
  return true;
}

ZooKeeperConfig::~ZooKeeperConfig() {
  close();
}

bool ZooKeeperConfig::getService(const std::string& serviceName,
                                 const std::string& options,
                                 server_vector_t& _return) {
  return false;
}


#endif
