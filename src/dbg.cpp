#include "dbg.h"

#include <boost/thread/locks.hpp>
#include <boost/thread/lock_guard.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>


namespace dbg
{

void MsgEventLogger::close()
{
    if (! m_is_enabled)
        return;
    boost::lock_guard<boost::mutex> guard(m_files_mutex);
    m_files.clear();
}

void MsgEventLogger::log(char const * event_code, std::string const & category, std::string const & msg)
{
    if (! m_is_enabled)
        return;
    CategoryEvent files_key(category, std::string(event_code));
    std::string msg_head = msg.substr(0, m_header_length);
    std::string::size_type nl_idx = msg_head.find('\n');
    if (nl_idx != std::string::npos)    // unlikely
        msg_head = msg_head.substr(0, nl_idx);
    
    // get file ref & mutex
    boost::shared_ptr<boost::mutex> fhlock;
    boost::shared_ptr<std::ofstream> fhptr;
    {
        // potential modification of m_files, so guarded
        boost::lock_guard<boost::mutex> guard(m_files_mutex);
        EventLogFiles::iterator files_it = m_files.find(files_key);
        if (m_files.end() == files_it)
        {
            FileRef newfile(m_dir + "/event_log." + files_key.event() + "." + files_key.category() + ".log");
            files_it = m_files.insert(EventLogFiles::value_type(files_key, newfile)).first;
        }
        fhlock = files_it->second.lock();
        fhptr = files_it->second.fh();
    }
    
    // write to logfile
    boost::lock_guard<boost::mutex> guard(*fhlock);
    *fhptr << boost::posix_time::second_clock::local_time() << " " << msg_head << std::endl;
}

} // namespace dbg
