#ifndef __SCRIBE_STORE_BUCKET_H__
#define __SCRIBE_STORE_BUCKET_H__

#include "store.h"

/*
 * This store separates messages into many groups based on a
 * hash function, and sends each group to a different store.
 */
class BucketStore : public Store {

 public:
  BucketStore(const std::string& category, bool multi_category);
  ~BucketStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  bool open();
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();
  void flush();
  void periodicCheck();

  std::string getStatus();

 protected:
  enum bucketizer_type {
    context_log,
    random,      // randomly hash messages without using any key
    key_hash,    // use hashing to split keys into buckets
    key_modulo,  // use modulo to split keys into buckets
    key_range    // use bucketRange to compute modulo to split keys into buckets
  };

  bucketizer_type bucketType;
  char delimiter;
  bool removeKey;
  bool opened;
  unsigned long bucketRange;  // used to compute key_range bucketizing
  unsigned long numBuckets;
  std::vector<boost::shared_ptr<Store> > buckets;

  unsigned long bucketize(const std::string& message);
  std::string getMessageWithoutKey(const std::string& message);

 private:
  // disallow copy, assignment, and emtpy construction
  BucketStore();
  BucketStore(BucketStore& rhs);
  BucketStore& operator=(BucketStore& rhs);
  void createBucketsFromBucket(pStoreConf configuration,
                               pStoreConf bucket_conf);
  void createBuckets(pStoreConf configuration);
};

#endif
