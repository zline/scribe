#ifndef SCRIBE_URL_H
#define SCRIBE_URL_H

#include <string>

/**
 * Basic URL parser. 
 * URLs must have the format PROTOCOL://HOST:PORT/PATH
 *
 * @author(jcorwin)
 */
class Url {
public:
    Url(const std::string & spec);
    Url(const std::string & protocol, const std::string & host, int port, const std::string & file);

    bool parseSuccessful() const;
    const std::string & getProtocol() const;
    const std::string & getHost() const;
    int getPort() const;
    const std::string & getFile() const;

protected:
    std::string protocol;
    std::string host;
    int port;
    std::string file;

private:
    bool parseStatus;
    static std::string extractMatch(const std::string & input, regmatch_t * match);
};

#endif
