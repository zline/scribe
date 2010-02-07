#include "store_multifile.h"

MultiFileStore::MultiFileStore(const std::string& category, bool multi_category)
  : CategoryStore(category, "MultiFileStore", multi_category) {
}

MultiFileStore::~MultiFileStore() {
}

void MultiFileStore::configure(pStoreConf configuration) {
  configureCommon(configuration, "file");
}
