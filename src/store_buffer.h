#ifndef __SCRIBE_STORE_BUFFER_H__
#define __SCRIBE_STORE_BUFFER_H__

#include "store.h"

/*
 * This store aggregates messages and sends them to another store
 * in larger groups. If it is unable to do this it saves them to
 * a secondary store, then reads them and sends them to the
 * primary store when it's back online.
 *
 * This actually involves two buffers. Messages are always buffered
 * briefly in memory, then they're buffered to a secondary store if
 * the primary store is down.
 */
class BufferStore : public Store {

 public:
  BufferStore(const std::string& category, bool multi_category);
  ~BufferStore();

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
  // Store we're trying to get the messages to
  boost::shared_ptr<Store> primaryStore;

  // Store to use as a buffer if the primary is unavailable.
  // The store must be of a type that supports reading.
  boost::shared_ptr<Store> secondaryStore;

  // buffer state machine
  enum buffer_state_t {
    STREAMING,       // connected to primary and sending directly
    DISCONNECTED,    // disconnected and writing to secondary
    SENDING_BUFFER,  // connected to primary and sending data from secondary
  };

  void changeState(buffer_state_t new_state); // handles state pre and post conditions
  const char* stateAsString(buffer_state_t state);

  time_t getNewRetryInterval(); // generates a random interval based on config

  // configuration
  unsigned long maxQueueLength;   // in number of messages
  unsigned long bufferSendRate;   // number of buffer files sent each periodicCheck
  time_t avgRetryInterval;        // in seconds, for retrying primary store open
  time_t retryIntervalRange;      // in seconds
  bool   replayBuffer;            // whether to send buffers from
                                  // secondary store to primary

  // state
  buffer_state_t state;
  time_t lastWriteTime;
  time_t lastOpenAttempt;
  time_t retryInterval;

 private:
  // disallow copy, assignment, and empty construction
  BufferStore();
  BufferStore(BufferStore& rhs);
  BufferStore& operator=(BufferStore& rhs);
};

#endif
