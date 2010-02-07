#ifndef __SCRIBE_STORE_CATEGORY_H__
#define __SCRIBE_STORE_CATEGORY_H__

#include "store.h"

/*
 * This store will contain a separate store for every distinct
 * category it encounters.
 *
 */
class CategoryStore : public Store {
 public:
  CategoryStore(const std::string& category, bool multi_category);
  CategoryStore(const std::string& category, const std::string& name,
                bool multiCategory);
  ~CategoryStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool open();
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();

  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  void periodicCheck();
  void flush();

 protected:
  void configureCommon(pStoreConf configuration, const std::string type);
  boost::shared_ptr<Store> modelStore;
  std::map<std::string, boost::shared_ptr<Store> > stores;

 private:
  CategoryStore();
  CategoryStore(Store& rhs);
  CategoryStore& operator=(Store& rhs);
};

#endif
