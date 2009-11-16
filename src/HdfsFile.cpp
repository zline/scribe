// Copyright (c) 2009- Facebook
// Distributed under the Scribe Software License
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//

#include "common.h"
#include "file.h"
#include "HdfsFile.h"

#ifdef USE_SCRIBE_HDFS

#ifdef LZO_STREAMING
/* magic file header for lzopack-compressed files */
static const unsigned char lzop_magic[9] =
  { 0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a };

/* LZOP default == 256k */
static const lzo_uint lzo_block_size = 256 * 1024l; 

/* compute a checksum */

#define F_ADLER32_D     0x00000001L
#define F_ADLER32_C     0x00000002L

static const lzo_uint32 lzo_flags = F_ADLER32_D | F_ADLER32_C;

/* LZO1X */
static const int lzo_method = 1;
#endif

using namespace std;

HdfsFile::HdfsFile(const std::string& name) : FileInterface(name, false), inputBuffer_(NULL), bufferSize_(0) {
  LOG_OPER("[hdfs] Connecting to HDFS");

  char *noJVMSignalHandlersPlease = "LIBHDFS_OPTS=-Xrs";
  LOG_OPER("[hdfs] Exporting %s", noJVMSignalHandlersPlease);

  putenv(strdup(noJVMSignalHandlersPlease));

  // First attempt to parse the hdfs cluster from the path name specified.
  // If it fails, then use the default hdfs cluster.
  fileSys = connectToPath(name.c_str());
  if (!fileSys) {
    LOG_OPER("[hdfs] Connected to default HDFS");

    fileSys = hdfsConnectNewInstance("default", 0);
  }

  if (fileSys == 0) {
    // ideally, we should throw an exception here, but the scribe store code
    // does not handle this elegantly now.
    LOG_OPER("[hdfs] ERROR: HDFS is not configured for file: %s", name.c_str());
  }
  hfile = 0;
}

HdfsFile::~HdfsFile() {
  if (fileSys) {
    hdfsDisconnect(fileSys);
  }
  fileSys = 0;
  hfile = 0;
}

bool HdfsFile::openRead() {
  if (fileSys) {
    hfile = hdfsOpenFile(fileSys, filename.c_str(), O_RDONLY, 0, 0, 0);
  }
  if (hfile) {
    LOG_OPER("[hdfs] opened for read %s", filename.c_str());
    return true;
  }
  return false;
}

#ifdef LZO_STREAMING
bool HdfsFile::LZOStringAppendChar(std::string&str, int c) {
  unsigned char cc = (unsigned char) (c & 0xff);
  lzo_checksum = lzo_adler32(lzo_checksum, &cc, 1);
  str.append((const char *)&cc, 1);
  return true;
}

bool HdfsFile::LZOStringAppendInt32(std::string&str, lzo_xint v) {
  unsigned char b[4];
  
  b[3] = (unsigned char) ((v >>  0) & 0xff);
  b[2] = (unsigned char) ((v >>  8) & 0xff);
  b[1] = (unsigned char) ((v >> 16) & 0xff);
  b[0] = (unsigned char) ((v >> 24) & 0xff);

  lzo_checksum = lzo_adler32(lzo_checksum, b, 4);
  str.append((const char*)b, 4);

  return true;
}

bool HdfsFile::LZOStringAppendInt16(std::string&str, unsigned v) {
  unsigned char b[2];

  b[1] = (unsigned char) (v >>  0);
  b[0] = (unsigned char) (v >>  8);

  lzo_checksum = lzo_adler32(lzo_checksum, b, 2);
  str.append((const char*)b, 2);

  return true;
}

#endif

bool HdfsFile::openWrite() {
  int flags;

  if (!fileSys) {
    return false;
  }

  if (hfile) {
    LOG_OPER("[hdfs] already opened for write %s", filename.c_str());
    return false;
  }

  const std::string base_filename = (LZOCompressionLevel > 0) ? 
    filename.substr(0,-3) : filename;

  if (hdfsExists(fileSys, filename.c_str()) == 0) {
    flags = O_WRONLY|O_APPEND; // file exists, append to it.
    /* Turn off compression for appends (for now) */
    if(LZOCompressionLevel != 0) {
      LOG_OPER("[hdfs] Turning off LZO compression for append operations");
      LZOCompressionLevel = 0;
    }
  } else {
    flags = O_WRONLY;
  }
  
  hfile = hdfsOpenFile(fileSys, filename.c_str(), flags, 0, 0, 0);
  if (hfile) {
    if (flags & O_APPEND) {
      LOG_OPER("[hdfs] opened for append %s", filename.c_str());
    } else {
      LOG_OPER("[hdfs] opened for write %s", filename.c_str());
      if(LZOCompressionLevel != 0) {
	LOG_OPER("[hdfs] writing LZO header to %s", filename.c_str());

	std::string lzo_header((const char*)lzop_magic, sizeof(lzop_magic));

	lzo_checksum = 1; // <- LZOP lzo_adler32(0, NULL, 0);  <- LZO

#define LZOP_VERSION 0x1010
	unsigned lzop_version = LZOP_VERSION & 0xffff;
	LZOStringAppendInt16(lzo_header, lzop_version);

	unsigned lzop_lib_version = lzo_version() & 0xffff;
	LZOStringAppendInt16(lzo_header, lzop_lib_version);

	/* Note this version has no crc-32 support or filter support */
	unsigned lzop_version_needed_to_extract = 0x0940;
	LZOStringAppendInt16(lzo_header, lzop_version_needed_to_extract);

	/* 8-bit int, but char works */
	LZOStringAppendChar(lzo_header, lzo_method);
	LZOStringAppendChar(lzo_header, LZOCompressionLevel);
	LZOStringAppendInt32(lzo_header, lzo_flags);

	/* write mode, mtime, gmtdiff, length of name, name, adler32 init (1)*/
	LZOStringAppendInt32(lzo_header, 0664); /* mode */
	LZOStringAppendInt32(lzo_header, 0); /* mtime */
	LZOStringAppendInt32(lzo_header, 0); /* gmtdiff */
	
	const char *baseFile = basename(base_filename.c_str());

	LZOStringAppendChar(lzo_header, strlen(baseFile));

	lzo_header.append(baseFile);
	lzo_checksum = lzo_adler32(lzo_checksum, 
				   (unsigned char*)baseFile, strlen(baseFile));
	LZOStringAppendInt32(lzo_header, lzo_checksum);

	tSize bytesWritten = hdfsWrite(fileSys, hfile, 
				       lzo_header.data(),
				       (tSize) lzo_header.length());

	if(bytesWritten != (tSize)lzo_header.length()) {
	  LOG_OPER("[hdfs] Failed writing LZO header");
	}
	LZObacklogBuffer = std::string("");
      }
    }
    return true;
  }
  return false;
}

bool HdfsFile::openTruncate() {
  LOG_OPER("[hdfs] truncate %s", filename.c_str());
  deleteFile();
  return openWrite();
}

bool HdfsFile::isOpen() {
   bool retVal = (hfile) ? true : false;
   return retVal;
}

void HdfsFile::close() {
  if (fileSys) {
    if (hfile) {
      if(LZOCompressionLevel != 0) {
	/* flush LZO buffer */
	bool good = false;
	const std::string& clearBuf = LZOCompress("", true, &good);

	if(good == true) {
	  hdfsWrite(fileSys, hfile, clearBuf.data(), clearBuf.length());
	}

	/* write EOF */
	unsigned int eof = 0;
	hdfsWrite(fileSys, hfile, &eof, sizeof(unsigned int));
      }

      hdfsCloseFile(fileSys, hfile);
      LOG_OPER("[hdfs] closed %s", filename.c_str());
    } else {
      LOG_OPER("[hdfs] No hfile!  So no write/flush!");
    }
    hfile = 0;
  } else {
    LOG_OPER("Filesystem closed on us?! WTF");
  }
}

void HdfsFile::setShouldLZOCompress(int compressionLevel) {
  // Initialize LZO and buffers

  LOG_OPER("[hdfs] setting LZO compression level to %d", compressionLevel);
  LZOCompressionLevel = compressionLevel;

  if (lzo_init() != LZO_E_OK) {
      LOG_OPER("[hdfs] LZO internal error - lzo_init() failed !!!");
      LZOCompressionLevel = 0;
  }
}

const std::string HdfsFile::LZOCompress(const std::string& inputData, bool force, bool *success) {
  *success = true;

  // If we have a previous buffer, prepend it to our input
  std::string data = std::string(inputData.data());
  if(LZObacklogBuffer.length() > 0) {
    data = LZObacklogBuffer + string(inputData.data());

    /* and reset the pending data to an empty string */
    LZObacklogBuffer = std::string("");
  }

  /*
     If the data we've received is smaller than
     lzo_block_size (256k by default), queue it and return the input

     if 'force == true', then don't queue it, and compress it
  */
  if(data.length() < lzo_block_size && force == false) {
    LZObacklogBuffer = data;
    return inputData;
  }

  /* work buffers */
  lzo_bytep out = NULL;
  lzo_bytep wrkmem = NULL;
  lzo_uint out_len = 0;
  lzo_uint32 wrk_len = 0;
  std::string compressedData("");

  if (LZOCompressionLevel == 9)
    wrk_len = LZO1X_999_MEM_COMPRESS;
  else
    wrk_len = LZO1X_1_MEM_COMPRESS;

  int blocks = (int)(data.length() / lzo_block_size);
  int total_data_length = blocks * lzo_block_size;

  if(blocks == 0)
    total_data_length = data.length();
  
  /*
   *  allocate compression buffers and work-memory
   */ 
  out = (lzo_bytep) calloc(1, lzo_block_size + lzo_block_size / 16 + 64 + 3);
  wrkmem = (lzo_bytep) calloc(1, wrk_len);
  if (out == NULL || wrkmem == NULL) {
    LOG_OPER("[hdfs] calloc failed");
    *success = false;
    return data;
  }

  /* 
   *  Compress each block and write it
   */ 
  int r = 0;
  for (int i = 0; i < blocks || blocks == 0; i++) {
      int offset = i * lzo_block_size;
      int data_len = blocks == 0 ? total_data_length : lzo_block_size;

      /* Let last block be a little bigger before we flush */
      if(i == blocks) {
	data_len += data.length() - total_data_length;
      }
      const std::string block_data = data.substr(offset, data_len);

      /* compress block */
      if (LZOCompressionLevel == 9)
	r = lzo1x_999_compress((const unsigned char *)block_data.data(), 
			       block_data.length(),
			       out, &out_len, wrkmem);
      else
	r = lzo1x_1_compress((const unsigned char *)block_data.data(), 
			     block_data.length(), 
			     out, &out_len, wrkmem);

      /* this should never occur */
      if (r != LZO_E_OK || 
	  out_len > block_data.length() + block_data.length() / 16 + 64 + 3) {
	LOG_OPER("[hdfs] LZO internal error - compression failed: %d", r);
	*success = false;
	return data;
      }

      /* write uncompressed block size */
      LZOStringAppendInt32(compressedData, block_data.length()); 

      /* write all the checksums and block sizes only if compression 
	 was useful (resulted in a smaller data set) */
      if(out_len < block_data.length()) {
	/* write compressed block size */
	lzo_uint32 out_len32 = out_len;

	LZOStringAppendInt32(compressedData, out_len32);

	/* write uncompressed block checksum */
	lzo_uint32 d_checksum = lzo_adler32(1,  // ADLER32_INIT_VALUE
					   (unsigned char *)block_data.data(), 
					   block_data.length());
	LZOStringAppendInt32(compressedData, d_checksum);

	/* write compressed block checksum */
	lzo_uint32 c_checksum = lzo_adler32(1, out, out_len32);
	LZOStringAppendInt32(compressedData, c_checksum);

	/* write compressed block data */
	compressedData.append((const char *)out, out_len);
      } 
      /* Otherwise, just write the raw block */
      else {
	LZOStringAppendInt32(compressedData, block_data.length());
	compressedData.append(block_data);
      }
      if(blocks == 0)
	break;
  }

  free(out);        out = NULL;
  free(wrkmem);  wrkmem = NULL;
  
  return compressedData;
}

bool HdfsFile::write(const std::string& data) {
  if (!isOpen()) {
    bool success = openWrite();

    if (!success) {
      return false;
    }
  }

  tSize bytesWritten = 0;
  /*
    'success' indicates whether a buffer was compressed and returned.
    a compressed buffer will only be returned once the lzo_block_size 
    has been reached.  until then the data is buffered/queued for compression.
  */
  bool success = false;

  if(LZOCompressionLevel != 0) {
    const std::string& compressedData = LZOCompress(data, false, &success);
    if(compressedData != data && success == true) {
      /* compression failed or the data is not compressible */
      bytesWritten = hdfsWrite(fileSys, hfile, compressedData.data(),
			       (tSize) compressedData.length());

      return (bytesWritten == (tSize)compressedData.length());
    } else if(success == false) {
      LOG_OPER("Compression failed, writing raw block");
      bytesWritten = hdfsWrite(fileSys, hfile, data.data(),
			       (tSize) data.length());
      
      return (bytesWritten == (tSize) data.length()) ? true : false;
    }
  } else {
    /* normal unbuffered/compressed write */
    bytesWritten = hdfsWrite(fileSys, hfile, data.data(),
			     (tSize) data.length());
    return (bytesWritten == (tSize) data.length()) ? true : false;
  }

  return success;
}

void HdfsFile::flush() {
  if (hfile) {
    hdfsFlush(fileSys, hfile);
  }
}

unsigned long HdfsFile::fileSize() {
  long size = 0L;

  if (fileSys) {
    hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys, filename.c_str());
    if (pFileInfo != NULL) {
      size = pFileInfo->mSize;
      hdfsFreeFileInfo(pFileInfo, 1);
    }
  }
  return size;
}

void HdfsFile::deleteFile() {
  if (fileSys) {
    hdfsDelete(fileSys, filename.c_str());
  }
  LOG_OPER("[hdfs] deleteFile %s", filename.c_str());
}

void HdfsFile::listImpl(const std::string& path,
                        std::vector<std::string>& _return) {
  if (!fileSys) {
    return;
  }

  int value = hdfsExists(fileSys, path.c_str());
  if (value == 0) {
    int numEntries = 0;
    hdfsFileInfo* pHdfsFileInfo = 0;
    pHdfsFileInfo = hdfsListDirectory(fileSys, path.c_str(), &numEntries);
    if (pHdfsFileInfo) {
      for(int i = 0; i < numEntries; i++) {
        char* pathname = pHdfsFileInfo[i].mName;
        char* filename = rindex(pathname, '/');
        if (filename != NULL) {
          _return.push_back(filename+1);
        }
      }
      hdfsFreeFileInfo(pHdfsFileInfo, numEntries);
    }
  }
}

bool HdfsFile::readNext(std::string& _return) {
   return false;           // frames not yet supported
}

string HdfsFile::getFrame(unsigned data_length) {
  return std::string();    // not supported
}

bool HdfsFile::createDirectory(std::string path) {
  // opening the file will create the directories.
  return true;
}

/**
 * HDFS currently does not support symlinks. So we create a
 * normal file and write the symlink data into it
 */
bool HdfsFile::createSymlink(std::string oldpath, std::string newpath) {
  LOG_OPER("[hdfs] Creating symlink oldpath %s newpath %s",
           oldpath.c_str(), newpath.c_str());
  HdfsFile* link = new HdfsFile(newpath);
  if (link->openWrite() == false) {
    LOG_OPER("[hdfs] Creating symlink failed because %s already exists.",
             newpath.c_str());
    return false;
  }
  if (link->write(oldpath) == false) {
    LOG_OPER("[hdfs] Writing symlink %s failed", newpath.c_str());
    return false;
  }
  link->close();
  return true;
}

/**
 * If the URI is specified of the form
 * hdfs://server::port/path, then connect to the
 * specified cluster
 */
hdfsFS HdfsFile::connectToPath(const char* uri) {
  char* defaultport = "default:0";
  char* hostport = (char *)malloc(strlen(uri) + strlen(defaultport) + 1);
  char* buf = (char *)malloc(strlen(uri) + strlen(defaultport) + 1);
  char* portStr;
  char* host;
  int   port = -1;
  hdfsFS fs = NULL;
  int ret = 0;

  if (uri == NULL || strlen(uri) == 0) {
    free (buf);
    free(hostport);
    return 0;
  }
  ret = sscanf(uri, "hdfs://%s", hostport);
  if (ret != 1) {
    // The pathname does not have a specified cluster.
    // Use default cluster.
    strcpy(hostport, defaultport);
  }
  host = strtok_r(hostport, ":", &buf);
  portStr = strtok_r(NULL, "/", &buf);
  if (host == NULL || portStr == NULL) {
    free(hostport);
    free (buf);
    return 0;
  }
  ret = sscanf(portStr, "%d", &port);
  if (ret != 1) {
    free(hostport);
    free (buf);
    return 0;
  }
  fs = hdfsConnectNewInstance(host, port);
  free(hostport);
  free(buf);
  return fs;
}

#endif // USE_SCRIBE_HDFS

