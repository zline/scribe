#include "url.h"

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

class UrlTest : public CppUnit::TestCase {
public:
    CPPUNIT_TEST_SUITE(UrlTest);
    CPPUNIT_TEST(testBase);
    CPPUNIT_TEST(testParse);
    CPPUNIT_TEST(testInvalid);
    CPPUNIT_TEST_SUITE_END();

    void testBase() {
        const std::string HTTP = "http";
        const std::string LOCALHOST = "localhost";
        const int PORT = 80;
        const std::string INDEX = "index.html";

        Url url(HTTP, LOCALHOST, PORT, INDEX);
        CPPUNIT_ASSERT(url.parseSuccessful());
        CPPUNIT_ASSERT_EQUAL(HTTP, url.getProtocol());
        CPPUNIT_ASSERT_EQUAL(LOCALHOST, url.getHost());
        CPPUNIT_ASSERT_EQUAL(PORT, url.getPort());
        CPPUNIT_ASSERT_EQUAL(INDEX, url.getFile());
    }

    void testParse() {
        const std::string PROTO = "zk";
        const std::string HOST = "zookeeper.test.com";
        const int PORT = 2181;
        const std::string PATH = "/path1/path2";
        const std::string ZK_URL = "zk://zookeeper.test.com:2181/path1/path2";
        Url url(ZK_URL);
        CPPUNIT_ASSERT(url.parseSuccessful());
        CPPUNIT_ASSERT_EQUAL(PROTO, url.getProtocol());
        CPPUNIT_ASSERT_EQUAL(HOST, url.getHost());
        CPPUNIT_ASSERT_EQUAL(PORT, url.getPort());
        CPPUNIT_ASSERT_EQUAL(PATH, url.getFile());
    }

    void testInvalid() {
        Url url1("foo://bar");
        CPPUNIT_ASSERT(!url1.parseSuccessful());
        Url url2("http:/localhost:80/file.html");
        CPPUNIT_ASSERT(!url2.parseSuccessful());
        Url url3("zk:///zk.test.com:2181/path");
        CPPUNIT_ASSERT(!url3.parseSuccessful());
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(UrlTest);

int main(int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  runner.run();
  return 0;
}
