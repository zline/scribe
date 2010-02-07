#include "store_null.h"
#include "common.h"
#include "scribe_server.h"

NullStore::NullStore(const std::string& category, bool multi_category)
  : Store(category, "null", multi_category)
{}

NullStore::~NullStore() {
}

boost::shared_ptr<Store> NullStore::copy(const std::string &category) {
  NullStore *store = new NullStore(category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);
  return copied;
}

bool NullStore::open() {
  return true;
}

bool NullStore::isOpen() {
  return true;
}

void NullStore::configure(pStoreConf) {
}

void NullStore::close() {
}

bool NullStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  incCounter(categoryHandled, "ignored", messages->size());
  return true;
}

void NullStore::flush() {
}

bool NullStore::readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                       struct tm* now) {
  return true;
}

bool NullStore::replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                              struct tm* now) {
  return true;
}

void NullStore::deleteOldest(struct tm* now) {
}

bool NullStore::empty(struct tm* now) {
  return true;
}
