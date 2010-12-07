#include "url.h"

const std::string URL_REGEX = "([a-zA-Z]+)://([a-zA-Z.0-9]+):(\\d+)(/.*)";
const int URL_REGEX_GROUPS = 4;

Url::Url(const std::string & urlProtocol, const std::string & urlHost, int urlPort,
         const std::string & urlFile) :
    protocol(urlProtocol), host(urlHost), port(urlPort), file(urlFile), parseStatus(true) {
}

Url::Url(const std::string & spec) {
    regex_t regex;
    int result;
    parseStatus = false;

    result = regcomp(&regex, URL_REGEX.c_str(), 0);
    if (result != 0) {
        LOG_OPER("ERROR: Failed to compile regular expression");
        return;
    }
    regmatch_t groups[URL_REGEX_GROUPS];
    result = regexec(&regex, spec.c_str(), URL_REGEX_GROUPS, groups, 0);
    if (result != 0) {
        LOG_OPER("ERROR: URL string '%s' failed to parse", spec.c_str());
        return;
    }
    protocol = Url::extractMatch(spec, &groups[0]);
    host = Url::extractMatch(spec, &groups[1]);
    port = atoi(Url::extractMatch(spec, &groups[2]).c_str());
    file = Url::extractMatch(spec, &groups[3]);
    parseStatus = true;
}

std::string Url::extractMatch(const std::string & input, regmatch_t * match) {
    return input.substr(match->rm_so, match->rm_eo - match->rm_so);
}

const std::string & Url::getProtocol() const {
    return protocol;
}

const std::string & Url::getHost() const {
    return host;
}

int Url::getPort() const {
    return port;
}

const std::string & Url::getFile() const {
    return file;
}
