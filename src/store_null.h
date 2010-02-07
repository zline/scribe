#ifndef __SCRIBE_STORE_NULL_H__
#define __SCRIBE_STORE_NULL_H__

#include "store.h"

/*
 * This store intentionally left blank.
 */
class NullStore : public Store {

 public:
  NullStore(const std::string& category, bool multi_category);
  virtual ~NullStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool open();
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();

  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  void flush();

  // null stores are readable, but you never get anything
  virtual bool readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,                          struct tm* now);
  virtual bool replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                             struct tm* now);
  virtual void deleteOldest(struct tm* now);
  virtual bool empty(struct tm* now);


 private:
  // disallow empty constructor, copy and assignment
  NullStore();
  NullStore(Store& rhs);
  NullStore& operator=(Store& rhs);
};

#endif
