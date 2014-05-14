#include "sequential_test.h"

#include <boost/thread/locks.hpp> 
#include <boost/thread/lock_guard.hpp> 

#include <fstream>


namespace seqtest
{

void MsgLogger::log(std::string const & msg)
{
    boost::lock_guard<boost::mutex> guard(m_data_lock);
    std::string::size_type first_eol = msg.find('\n');  // log end of first and last message strings
    if (first_eol != std::string::npos && first_eol != msg.length() - 1)
        m_data.push_back(last_substr(msg, first_eol));
    
    m_data.push_back(last_substr(msg, msg.length()));
}
    
void MsgLogger::flush(std::string filename)
{
    std::ofstream of(filename.c_str());
    boost::lock_guard<boost::mutex> guard(m_data_lock);
    for (log_t::const_iterator it = m_data.begin(); it != m_data.end(); it++)
        of << *it << std::endl;
    
    clear();
}

void MsgLogger::clear()
{
    m_data.clear();
}

} // namespace seqtest
