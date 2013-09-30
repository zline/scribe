#ifndef __SEQUENTIAL_TEST_H_2013_09_30
#define __SEQUENTIAL_TEST_H_2013_09_30

#include <stdexcept>
#include <string>
#include <vector>

#include <pthread.h>


namespace seqtest
{

class RAIILock
{
public:
    RAIILock(pthread_mutex_t & lock): m_lock(lock)
    {
        if (pthread_mutex_lock(&m_lock))
            throw std::runtime_error("failed to pthread_mutex_lock");
    }
    ~RAIILock()
    {
        if (pthread_mutex_unlock(&m_lock))
            throw std::runtime_error("failed to pthread_mutex_unlock");
    }
    
private:
    pthread_mutex_t & m_lock;
};

/// @brief class accumulates in-memory log of message sequence numbers
class MsgLogger
{
public:
    MsgLogger(std::string::size_type last_bytes = std::string::size_type(17U));
    
    void log(std::string const & msg);
    void flush(std::string filename);
    void clear();
    
    ~MsgLogger();

private:
    std::string::size_type const m_last_bytes;
    
    typedef std::vector<std::string> log_t;
    log_t m_data;
    pthread_mutex_t m_data_lock;
    
    std::string last_substr(std::string const & str, std::string::size_type end)
    {
        std::string::size_type begin = (m_last_bytes > end) ? std::string::size_type(0U) : end - m_last_bytes;
        return str.substr(begin, end - begin);
    }
};

} // namespace seqtest

#endif // __SEQUENTIAL_TEST_H_2013_09_30
