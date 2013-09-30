#include "sequential_test.h"

#include <fstream>


namespace seqtest
{

MsgLogger::MsgLogger(std::string::size_type last_bytes): m_last_bytes(last_bytes)
{
    pthread_mutex_init(&m_data_lock, NULL);
}
    
void MsgLogger::log(std::string const & msg)
{
    RAIILock lock(m_data_lock);
    std::string::size_type first_eol = msg.find('\n');  // log end of first and last message strings
    if (first_eol != std::string::npos && first_eol != msg.length() - 1)
        m_data.push_back(last_substr(msg, first_eol));
    
    m_data.push_back(last_substr(msg, msg.length()));
}
    
void MsgLogger::flush(std::string filename)
{
    std::ofstream of(filename.c_str());
    RAIILock lock(m_data_lock);
    for (log_t::const_iterator it = m_data.begin(); it != m_data.end(); it++)
        of << *it << std::endl;
    
    clear();
}

void MsgLogger::clear()
{
    m_data.clear();
}

MsgLogger::~MsgLogger()
{
    pthread_mutex_destroy(&m_data_lock);
}

} // namespace seqtest
