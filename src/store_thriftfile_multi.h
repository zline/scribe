#ifndef __SCRIBE_STORE_THRIFTFILE_MULTI_H__
#define __SCRIBE_STORE_THRIFTFILE_MULTI_H__

#include "store_category.h"
/*
 * ThriftMultiFileStore is similar to ThriftFileStore except that it uses a
 * separate thrift file for every category.  This is useful only if this store
 * can handle mutliple categories.
 */
class ThriftMultiFileStore : public CategoryStore {
 public:
  ThriftMultiFileStore(const std::string& category, bool multi_category);
  ~ThriftMultiFileStore();
  void configure(pStoreConf configuration);


 private:
  ThriftMultiFileStore();
  ThriftMultiFileStore(Store& rhs);
  ThriftMultiFileStore& operator=(Store& rhs);
};

#endif
