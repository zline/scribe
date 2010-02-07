#ifndef __SCRIBE_STORE_MULTI_H__
#define __SCRIBE_STORE_MULTI_H__

#include "store.h"

/*
 * This store relays messages to n other stores
 * @author Joel Seligstein
 */
class MultiStore : public Store {
 public:
  MultiStore(const std::string& category, bool multi_category);
  ~MultiStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool open();
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();

  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  void periodicCheck();
  void flush();

  // read won't make sense since we don't know which store to read from
  bool readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                  struct tm* now) { return false; }
  void deleteOldest(struct tm* now) {}
  bool empty(struct tm* now) { return true; }

 protected:
  std::vector<boost::shared_ptr<Store> > stores;
  enum report_success_value {
    SUCCESS_ANY = 1,
    SUCCESS_ALL
  };
  report_success_value report_success;

 private:
  // disallow copy, assignment, and empty construction
  MultiStore();
  MultiStore(Store& rhs);
  MultiStore& operator=(Store& rhs);
};

#endif
