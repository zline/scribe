#ifndef __SCRIBE_STORE_MULTIFILE_H__
#define __SCRIBE_STORE_MULTIFILE_H__

#include "store_category.h"

/*
 * MultiFileStore is similar to FileStore except that it uses a separate file
 * for every category.  This is useful only if this store can handle mutliple
 * categories.
 */
class MultiFileStore : public CategoryStore {
 public:
  MultiFileStore(const std::string& category, bool multi_category);
  ~MultiFileStore();
  void configure(pStoreConf configuration);

 private:
  MultiFileStore();
  MultiFileStore(Store& rhs);
  MultiFileStore& operator=(Store& rhs);
};

#endif
