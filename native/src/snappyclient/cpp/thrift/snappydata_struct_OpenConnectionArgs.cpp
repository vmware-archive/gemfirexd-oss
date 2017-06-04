/**
 * Autogenerated by Thrift Compiler (0.10.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */

#include <iosfwd>

#include <thrift/Thrift.h>
#include <thrift/TApplicationException.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TTransport.h>

#include <thrift/cxxfunctional.h>
#include "snappydata_struct_OpenConnectionArgs.h"

#include <algorithm>
#include <ostream>

#include <thrift/TToString.h>

namespace io { namespace snappydata { namespace thrift {


OpenConnectionArgs::~OpenConnectionArgs() noexcept {
}


void OpenConnectionArgs::__set_clientHostName(const std::string& val) {
  this->clientHostName = val;
}

void OpenConnectionArgs::__set_clientID(const std::string& val) {
  this->clientID = val;
}

void OpenConnectionArgs::__set_security(const SecurityMechanism::type val) {
  this->security = val;
}

void OpenConnectionArgs::__set_userName(const std::string& val) {
  this->userName = val;
__isset.userName = true;
}

void OpenConnectionArgs::__set_password(const std::string& val) {
  this->password = val;
__isset.password = true;
}

void OpenConnectionArgs::__set_forXA(const bool val) {
  this->forXA = val;
__isset.forXA = true;
}

void OpenConnectionArgs::__set_tokenSize(const int32_t val) {
  this->tokenSize = val;
__isset.tokenSize = true;
}

void OpenConnectionArgs::__set_useStringForDecimal(const bool val) {
  this->useStringForDecimal = val;
__isset.useStringForDecimal = true;
}

void OpenConnectionArgs::__set_properties(const std::map<std::string, std::string> & val) {
  this->properties = val;
__isset.properties = true;
}

uint32_t OpenConnectionArgs::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;

  bool isset_clientHostName = false;
  bool isset_clientID = false;
  bool isset_security = false;

  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->clientHostName);
          isset_clientHostName = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->clientID);
          isset_clientID = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 3:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          int32_t ecast111;
          xfer += iprot->readI32(ecast111);
          this->security = (SecurityMechanism::type)ecast111;
          isset_security = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 4:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->userName);
          this->__isset.userName = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 5:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->password);
          this->__isset.password = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 6:
        if (ftype == ::apache::thrift::protocol::T_BOOL) {
          xfer += iprot->readBool(this->forXA);
          this->__isset.forXA = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 7:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          xfer += iprot->readI32(this->tokenSize);
          this->__isset.tokenSize = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 8:
        if (ftype == ::apache::thrift::protocol::T_BOOL) {
          xfer += iprot->readBool(this->useStringForDecimal);
          this->__isset.useStringForDecimal = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 9:
        if (ftype == ::apache::thrift::protocol::T_MAP) {
          {
            this->properties.clear();
            uint32_t _size112;
            ::apache::thrift::protocol::TType _ktype113;
            ::apache::thrift::protocol::TType _vtype114;
            xfer += iprot->readMapBegin(_ktype113, _vtype114, _size112);
            uint32_t _i116;
            for (_i116 = 0; _i116 < _size112; ++_i116)
            {
              std::string _key117;
              xfer += iprot->readString(_key117);
              std::string& _val118 = this->properties[_key117];
              xfer += iprot->readString(_val118);
            }
            xfer += iprot->readMapEnd();
          }
          this->__isset.properties = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  if (!isset_clientHostName)
    throw TProtocolException(TProtocolException::INVALID_DATA);
  if (!isset_clientID)
    throw TProtocolException(TProtocolException::INVALID_DATA);
  if (!isset_security)
    throw TProtocolException(TProtocolException::INVALID_DATA);
  return xfer;
}

uint32_t OpenConnectionArgs::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("OpenConnectionArgs");

  xfer += oprot->writeFieldBegin("clientHostName", ::apache::thrift::protocol::T_STRING, 1);
  xfer += oprot->writeString(this->clientHostName);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("clientID", ::apache::thrift::protocol::T_STRING, 2);
  xfer += oprot->writeString(this->clientID);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("security", ::apache::thrift::protocol::T_I32, 3);
  xfer += oprot->writeI32((int32_t)this->security);
  xfer += oprot->writeFieldEnd();

  if (this->__isset.userName) {
    xfer += oprot->writeFieldBegin("userName", ::apache::thrift::protocol::T_STRING, 4);
    xfer += oprot->writeString(this->userName);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.password) {
    xfer += oprot->writeFieldBegin("password", ::apache::thrift::protocol::T_STRING, 5);
    xfer += oprot->writeString(this->password);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.forXA) {
    xfer += oprot->writeFieldBegin("forXA", ::apache::thrift::protocol::T_BOOL, 6);
    xfer += oprot->writeBool(this->forXA);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.tokenSize) {
    xfer += oprot->writeFieldBegin("tokenSize", ::apache::thrift::protocol::T_I32, 7);
    xfer += oprot->writeI32(this->tokenSize);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.useStringForDecimal) {
    xfer += oprot->writeFieldBegin("useStringForDecimal", ::apache::thrift::protocol::T_BOOL, 8);
    xfer += oprot->writeBool(this->useStringForDecimal);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.properties) {
    xfer += oprot->writeFieldBegin("properties", ::apache::thrift::protocol::T_MAP, 9);
    {
      xfer += oprot->writeMapBegin(::apache::thrift::protocol::T_STRING, ::apache::thrift::protocol::T_STRING, static_cast<uint32_t>(this->properties.size()));
      std::map<std::string, std::string> ::const_iterator _iter119;
      for (_iter119 = this->properties.begin(); _iter119 != this->properties.end(); ++_iter119)
      {
        xfer += oprot->writeString(_iter119->first);
        xfer += oprot->writeString(_iter119->second);
      }
      xfer += oprot->writeMapEnd();
    }
    xfer += oprot->writeFieldEnd();
  }
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

void swap(OpenConnectionArgs &a, OpenConnectionArgs &b) noexcept {
  using ::std::swap;
  static_assert(noexcept(swap(a, b)), "throwing swap");
  swap(a.clientHostName, b.clientHostName);
  swap(a.clientID, b.clientID);
  swap(a.security, b.security);
  swap(a.userName, b.userName);
  swap(a.password, b.password);
  swap(a.forXA, b.forXA);
  swap(a.tokenSize, b.tokenSize);
  swap(a.useStringForDecimal, b.useStringForDecimal);
  swap(a.properties, b.properties);
  swap(a.__isset, b.__isset);
}

OpenConnectionArgs::OpenConnectionArgs(const OpenConnectionArgs& other120) {
  clientHostName = other120.clientHostName;
  clientID = other120.clientID;
  security = other120.security;
  userName = other120.userName;
  password = other120.password;
  forXA = other120.forXA;
  tokenSize = other120.tokenSize;
  useStringForDecimal = other120.useStringForDecimal;
  properties = other120.properties;
  __isset = other120.__isset;
}
OpenConnectionArgs::OpenConnectionArgs( OpenConnectionArgs&& other121) noexcept {
  clientHostName = std::move(other121.clientHostName);
  clientID = std::move(other121.clientID);
  security = std::move(other121.security);
  userName = std::move(other121.userName);
  password = std::move(other121.password);
  forXA = std::move(other121.forXA);
  tokenSize = std::move(other121.tokenSize);
  useStringForDecimal = std::move(other121.useStringForDecimal);
  properties = std::move(other121.properties);
  __isset = std::move(other121.__isset);
}
OpenConnectionArgs& OpenConnectionArgs::operator=(const OpenConnectionArgs& other122) {
  clientHostName = other122.clientHostName;
  clientID = other122.clientID;
  security = other122.security;
  userName = other122.userName;
  password = other122.password;
  forXA = other122.forXA;
  tokenSize = other122.tokenSize;
  useStringForDecimal = other122.useStringForDecimal;
  properties = other122.properties;
  __isset = other122.__isset;
  return *this;
}
OpenConnectionArgs& OpenConnectionArgs::operator=(OpenConnectionArgs&& other123) noexcept {
  clientHostName = std::move(other123.clientHostName);
  clientID = std::move(other123.clientID);
  security = std::move(other123.security);
  userName = std::move(other123.userName);
  password = std::move(other123.password);
  forXA = std::move(other123.forXA);
  tokenSize = std::move(other123.tokenSize);
  useStringForDecimal = std::move(other123.useStringForDecimal);
  properties = std::move(other123.properties);
  __isset = std::move(other123.__isset);
  return *this;
}
void OpenConnectionArgs::printTo(std::ostream& out) const {
  using ::apache::thrift::to_string;
  out << "OpenConnectionArgs(";
  out << "clientHostName=" << to_string(clientHostName);
  out << ", " << "clientID=" << to_string(clientID);
  out << ", " << "security=" << to_string(security);
  out << ", " << "userName="; (__isset.userName ? (out << to_string(userName)) : (out << "<null>"));
  out << ", " << "password="; (__isset.password ? (out << to_string(password)) : (out << "<null>"));
  out << ", " << "forXA="; (__isset.forXA ? (out << to_string(forXA)) : (out << "<null>"));
  out << ", " << "tokenSize="; (__isset.tokenSize ? (out << to_string(tokenSize)) : (out << "<null>"));
  out << ", " << "useStringForDecimal="; (__isset.useStringForDecimal ? (out << to_string(useStringForDecimal)) : (out << "<null>"));
  out << ", " << "properties="; (__isset.properties ? (out << to_string(properties)) : (out << "<null>"));
  out << ")";
}

}}} // namespace