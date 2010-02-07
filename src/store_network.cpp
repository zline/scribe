
#include "store_network.h"
#include "conn_pool.h"
#include "network_config.h"

ConnPool g_connPool;

NetworkStore::NetworkStore(const string& category, bool multi_category)
  : Store(category, "network", multi_category),
    useConnPool(false),
    smcBased(false),
    remotePort(0),
    serviceCacheTimeout(DEFAULT_NETWORKSTORE_CACHE_TIMEOUT),
    lastServiceCheck(0),
    opened(false) {
  // we can't open the connection until we get configured

  // the bool for opened ensures that we don't make duplicate
  // close calls, which would screw up the connection pool's
  // reference counting.
}

NetworkStore::~NetworkStore() {
  close();
}

void NetworkStore::configure(pStoreConf configuration) {
  // Error checking is done on open()
  // smc takes precedence over host + port
  if (configuration->getString("smc_service", smcService)) {
    smcBased = true;

    // Constructor defaults are fine if these don't exist
    configuration->getString("service_options", serviceOptions);
    configuration->getUnsigned("service_cache_timeout", serviceCacheTimeout);
  } else {
    smcBased = false;
    configuration->getString("remote_host", remoteHost);
    configuration->getUnsigned("remote_port", remotePort);
  }

  if (!configuration->getInt("timeout", timeout)) {
    timeout = DEFAULT_SOCKET_TIMEOUT_MS;
  }

  string temp;
  if (configuration->getString("use_conn_pool", temp)) {
    if (0 == temp.compare("yes")) {
      useConnPool = true;
    }
  }
}

bool NetworkStore::open() {

  if (smcBased) {
    bool success = true;
    time_t now = time(NULL);

    // Only get list of servers if we haven't already gotten them recently
    if (lastServiceCheck <= (time_t) (now - serviceCacheTimeout)) {
      lastServiceCheck = now;

      success =
        network_config::getService(smcService, serviceOptions, servers);
    }

    // Cannot open if we couldn't find any servers
    if (!success || servers.empty()) {
      LOG_OPER("[%s] Failed to get servers from smc", categoryHandled.c_str());
      setStatus("Could not get list of servers from smc");
      return false;
    }

    if (useConnPool) {
      opened = g_connPool.open(smcService, servers, static_cast<int>(timeout));
    } else {
      // only open unpooled connection if not already open
      if (unpooledConn == NULL) {
        unpooledConn = shared_ptr<scribeConn>(new scribeConn(smcService, servers, static_cast<int>(timeout)));
        opened = unpooledConn->open();
      } else {
        opened = unpooledConn->isOpen();
        if (!opened) {
          opened = unpooledConn->open();
        }
      }
    }

  } else if (remotePort <= 0 ||
             remoteHost.empty()) {
    LOG_OPER("[%s] Bad config - won't attempt to connect to <%s:%lu>", categoryHandled.c_str(), remoteHost.c_str(), remotePort);
    setStatus("Bad config - invalid location for remote server");
    return false;

  } else {
    if (useConnPool) {
      opened = g_connPool.open(remoteHost, remotePort, static_cast<int>(timeout));
    } else {
      // only open unpooled connection if not already open
      if (unpooledConn == NULL) {
        unpooledConn = shared_ptr<scribeConn>(new scribeConn(remoteHost, remotePort, static_cast<int>(timeout)));
        opened = unpooledConn->open();
      } else {
        opened = unpooledConn->isOpen();
        if (!opened) {
          opened = unpooledConn->open();
        }
      }
    }
  }


  if (opened) {
    setStatus("");
  } else {
    setStatus("Failed to connect");
  }
  return opened;
}

void NetworkStore::close() {
  if (!opened) {
    return;
  }
  opened = false;
  if (useConnPool) {
    if (smcBased) {
      g_connPool.close(smcService);
    } else {
      g_connPool.close(remoteHost, remotePort);
    }
  } else {
    if (unpooledConn != NULL) {
      unpooledConn->close();
    }
  }
}

bool NetworkStore::isOpen() {
  return opened;
}

shared_ptr<Store> NetworkStore::copy(const std::string &category) {
  NetworkStore *store = new NetworkStore(category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->useConnPool = useConnPool;
  store->smcBased = smcBased;
  store->timeout = timeout;
  store->remoteHost = remoteHost;
  store->remotePort = remotePort;
  store->smcService = smcService;

  return copied;
}

bool NetworkStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  if (!isOpen()) {
    LOG_OPER("[%s] Logic error: NetworkStore::handleMessages called on closed store", categoryHandled.c_str());
    return false;
  } else if (useConnPool) {
    if (smcBased) {
      return g_connPool.send(smcService, messages);
    } else {
      return g_connPool.send(remoteHost, remotePort, messages);
    }
  } else {
    if (unpooledConn) {
      return unpooledConn->send(messages);
    } else {
      LOG_OPER("[%s] Logic error: NetworkStore::handleMessages unpooledConn is NULL", categoryHandled.c_str());
      return false;
    }
  }
}

void NetworkStore::flush() {
  // Nothing to do
}
