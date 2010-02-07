#ifndef __SCRIBE_STORE_FILE_H__
#define __SCRIBE_STORE_FILE_H__

#include "store.h"
#include "store_file_base.h"

/*
 * This file-based store uses an instance of a FileInterface class that
 * handles the details of interfacing with the filesystem. (see file.h)
 */
class FileStore : public FileStoreBase {

 public:
  FileStore(const std::string& category, bool multi_category,
            bool is_buffer_file = false);
  ~FileStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();
  void flush();

  // Each read does its own open and close and gets the whole file.
  // This is separate from the write file, and not really a consistent interface.
  bool readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                  struct tm* now);
  virtual bool replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                             struct tm* now);
  void deleteOldest(struct tm* now);
  bool empty(struct tm* now);

 protected:
  // Implement FileStoreBase virtual function
  bool openInternal(bool incrementFilename, struct tm* current_time);
  bool writeMessages(boost::shared_ptr<logentry_vector_t> messages,
                     boost::shared_ptr<FileInterface> write_file =
                     boost::shared_ptr<FileInterface>());

  bool isBufferFile;
  bool addNewlines;

  // State
  boost::shared_ptr<FileInterface> writeFile;

 private:
  // disallow copy, assignment, and empty construction
  FileStore(FileStore& rhs);
  FileStore& operator=(FileStore& rhs);
};

#endif
