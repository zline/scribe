#ifndef __SCRIBE_STORE_THRIFTFILE_H__
#define __SCRIBE_STORE_THRIFTFILE_H__

#include "store.h"
#include "store_file_base.h"

/*
 * This file-based store relies on thrift's TFileTransport to do the writing
 */
class ThriftFileStore : public FileStoreBase {
 public:
  ThriftFileStore(const std::string& category, bool multi_category);
  ~ThriftFileStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  bool open();
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();
  void flush();
  bool createFileDirectory();

 protected:
  // Implement FileStoreBase virtual function
  bool openInternal(bool incrementFilename, struct tm* current_time);

  boost::shared_ptr<apache::thrift::transport::TTransport> thriftFileTransport;

  unsigned long flushFrequencyMs;
  unsigned long msgBufferSize;
  unsigned long useSimpleFile;

 private:
  // disallow copy, assignment, and empty construction
  ThriftFileStore(ThriftFileStore& rhs);
  ThriftFileStore& operator=(ThriftFileStore& rhs);
};

#endif
