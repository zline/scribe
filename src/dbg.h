#ifndef __DBG_H_2014_05_14
#define __DBG_H_2014_05_14

#include <string>
#include <utility>
#include <map>
#include <fstream>

#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>


namespace dbg
{

/// @brief class logs message headers to a set of files partitioned by event type and message category
/// @note usage induces performance degradation
class MsgEventLogger
{
public:
    /// @brief constructs MsgEventLogger in disabled state
    MsgEventLogger(): m_is_enabled(false), m_dir(), m_header_length() {};
    
    MsgEventLogger(std::string dir, std::string::size_type header_length = std::string::size_type(100U)):
        m_is_enabled(true), m_dir(dir), m_header_length(header_length) {};
    
    void log(char const * event_code, std::string const & category, std::string const & msg);
    void close();
    
    ~MsgEventLogger() { close(); }

private:
    bool m_is_enabled;
    
    std::string const m_dir;
    std::string::size_type const m_header_length;
    
    typedef std::pair<std::string, std::string> CategoryEventBase;
    struct CategoryEvent : public CategoryEventBase
    {
        CategoryEvent(std::string category, std::string event): CategoryEventBase(category, event) {};
        std::string category() const { return first; }
        std::string event() const { return second; }
    };
    
    typedef std::pair< boost::shared_ptr<std::ofstream>, boost::shared_ptr<boost::mutex> > FileRefBase;
    struct FileRef : public FileRefBase
    {
        FileRef(std::string filename): FileRefBase(
            boost::make_shared<std::ofstream>(filename.c_str(), std::ios_base::out | std::ios_base::app),
            boost::make_shared<boost::mutex>()) {};
        
        boost::shared_ptr<std::ofstream> fh() const { return first; }
        boost::shared_ptr<boost::mutex> lock() const { return second; }
    };
    
    typedef std::map<CategoryEvent, FileRef> EventLogFiles;
    EventLogFiles m_files;
    boost::mutex m_files_mutex;
};

} // namespace dbg

#endif // __DBG_H_2014_05_14
