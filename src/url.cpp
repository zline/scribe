#include "url.h"

Url::Url(const std::string & urlProtocol, const std::string & urlHost, int urlPort,
         const std::string & urlFile) :
    protocol(urlProtocol), host(urlHost), port(urlPort), file(urlFile), parseStatus(true) {
}

Url::Url(const std::string & spec) {
    static const boost::regex urlRegex("([a-zA-Z]+)://([a-zA-Z.0-9]+):(\\d+)(/.*)");
    boost::cmatch match;
    if (boost::regex_match(spec, match, urlRegex)) {
        protocol = match[0];
        host = match[1];
        port = atoi(match[2].c_str());
        file = match[3];
        parseStatus = true;
    } else {
        parseStatus = false;
    }
}

const std::string & Url::getProtocol() {
    return protocol;
}

const std::string & Url::getHost() {
    return host;
}

int getPort() {
    return port;
}

const std::string & Url::getFile() {
    return file;
}
