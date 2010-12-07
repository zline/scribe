#ifndef SCRIBE_URL_H
#define SCRIBE_URL_H

#include "common.h"

class Url {
public:
    Url(const std::string & protocol, const std::string & host, int port, const std::string & file);
    Url(const std::string & spec);

    bool parseSuccessfull();
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
