#include "store_thriftfile_multi.h"

ThriftMultiFileStore::ThriftMultiFileStore(const std::string& category,
                                           bool multi_category)
  : CategoryStore(category, "ThriftMultiFileStore", multi_category) {
}

ThriftMultiFileStore::~ThriftMultiFileStore() {
}

void ThriftMultiFileStore::configure(pStoreConf configuration) {
  configureCommon(configuration, "thriftfile");
}
