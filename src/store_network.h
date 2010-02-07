#ifndef __SCRIBE_STORE_NETWORK_H__
#define __SCRIBE_STORE_NETWORK_H__

#define USE_ZOOKEEPER_SMC
#ifdef USE_ZOOKEEPER_SMC
#include "zookeeper.h"
#endif

#include "common.h" // includes std libs, thrift, and stl typedefs
#include "store.h"

/*
 * This store sends messages to another scribe server.
 * This class is really just an adapter to the global
 * connection pool g_connPool.
 */
class NetworkStore : public Store {

 public:
  NetworkStore(const std::string& category, bool multi_category);
  ~NetworkStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  bool open();
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();
  void flush();

 protected:
  static const long int DEFAULT_SOCKET_TIMEOUT_MS = 5000; // 5 sec timeout

  // configuration
  bool useConnPool;
  bool smcBased;
  long int timeout;
  std::string remoteHost;
  unsigned long remotePort; // long because it works with config code
  std::string smcService;
  std::string serviceOptions;
  server_vector_t servers;
  unsigned long serviceCacheTimeout;
  time_t lastServiceCheck;

  // state
  bool opened;
  boost::shared_ptr<scribeConn> unpooledConn; // null if useConnPool

 private:
  // disallow copy, assignment, and empty construction
  NetworkStore();
  NetworkStore(NetworkStore& rhs);
  NetworkStore& operator=(NetworkStore& rhs);
};

#endif
