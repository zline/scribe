#ifndef __SEQUENTIAL_TEST_H_2013_09_30
#define __SEQUENTIAL_TEST_H_2013_09_30

#include <stdexcept>
#include <string>
#include <vector>

#include <boost/thread/mutex.hpp>


namespace seqtest
{

/// @brief class accumulates in-memory log of message sequence numbers
class MsgLogger
{
public:
    MsgLogger(std::string::size_type last_bytes = std::string::size_type(17U)): m_last_bytes(last_bytes) {};
    
    void log(std::string const & msg);
    void flush(std::string filename);
    void clear();

private:
    std::string::size_type const m_last_bytes;
    
    typedef std::vector<std::string> log_t;
    log_t m_data;
    boost::mutex m_data_lock;
    
    std::string last_substr(std::string const & str, std::string::size_type end)
    {
        std::string::size_type begin = (m_last_bytes > end) ? std::string::size_type(0U) : end - m_last_bytes;
        return str.substr(begin, end - begin);
    }
};

} // namespace seqtest

#endif // __SEQUENTIAL_TEST_H_2013_09_30
