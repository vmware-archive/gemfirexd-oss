/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */

#include <thrift/Thrift.h>
#include <thrift/TApplicationException.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TTransport.h>

#include <thrift/cxxfunctional.h>

#include "gfxd_struct_FieldValue.h"

#include <algorithm>

namespace com { namespace pivotal { namespace gemfirexd { namespace thrift {

const char* FieldValue::ascii_fingerprint = "03979632577DF49328A71486E4E4C4E9";
const uint8_t FieldValue::binary_fingerprint[16] = {0x03,0x97,0x96,0x32,0x57,0x7D,0xF4,0x93,0x28,0xA7,0x14,0x86,0xE4,0xE4,0xC4,0xE9};

uint32_t FieldValue::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_BOOL) {
          xfer += iprot->readBool(this->bool_val);
          this->__isset.bool_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_BYTE) {
          xfer += iprot->readByte(this->byte_val);
          this->__isset.byte_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 3:
        if (ftype == ::apache::thrift::protocol::T_I16) {
          xfer += iprot->readI16(this->char_val);
          this->__isset.char_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 4:
        if (ftype == ::apache::thrift::protocol::T_I16) {
          xfer += iprot->readI16(this->short_val);
          this->__isset.short_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 5:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          xfer += iprot->readI32(this->int_val);
          this->__isset.int_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 6:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->long_val);
          this->__isset.long_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 7:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          xfer += iprot->readI32(this->float_val);
          this->__isset.float_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 8:
        if (ftype == ::apache::thrift::protocol::T_DOUBLE) {
          xfer += iprot->readDouble(this->double_val);
          this->__isset.double_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 9:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->string_val);
          this->__isset.string_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 10:
        if (ftype == ::apache::thrift::protocol::T_STRUCT) {
          xfer += this->decimal_val.read(iprot);
          this->__isset.decimal_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 11:
        if (ftype == ::apache::thrift::protocol::T_STRUCT) {
          xfer += this->timestamp_val.read(iprot);
          this->__isset.timestamp_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 12:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->bool_array.clear();
            uint32_t _size1;
            ::apache::thrift::protocol::TType _etype4;
            xfer += iprot->readListBegin(_etype4, _size1);
            this->bool_array.resize(_size1);
            uint32_t _i5;
            for (_i5 = 0; _i5 < _size1; ++_i5)
            {
              xfer += iprot->readBool(this->bool_array[_i5]);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.bool_array = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 13:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readBinary(this->byte_array);
          this->__isset.byte_array = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 14:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->char_array);
          this->__isset.char_array = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 15:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->short_array.clear();
            uint32_t _size6;
            ::apache::thrift::protocol::TType _etype9;
            xfer += iprot->readListBegin(_etype9, _size6);
            this->short_array.resize(_size6);
            uint32_t _i10;
            for (_i10 = 0; _i10 < _size6; ++_i10)
            {
              xfer += iprot->readI16(this->short_array[_i10]);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.short_array = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 16:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->int_array.clear();
            uint32_t _size11;
            ::apache::thrift::protocol::TType _etype14;
            xfer += iprot->readListBegin(_etype14, _size11);
            this->int_array.resize(_size11);
            uint32_t _i15;
            for (_i15 = 0; _i15 < _size11; ++_i15)
            {
              xfer += iprot->readI32(this->int_array[_i15]);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.int_array = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 17:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->long_array.clear();
            uint32_t _size16;
            ::apache::thrift::protocol::TType _etype19;
            xfer += iprot->readListBegin(_etype19, _size16);
            this->long_array.resize(_size16);
            uint32_t _i20;
            for (_i20 = 0; _i20 < _size16; ++_i20)
            {
              xfer += iprot->readI64(this->long_array[_i20]);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.long_array = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 18:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->float_array.clear();
            uint32_t _size21;
            ::apache::thrift::protocol::TType _etype24;
            xfer += iprot->readListBegin(_etype24, _size21);
            this->float_array.resize(_size21);
            uint32_t _i25;
            for (_i25 = 0; _i25 < _size21; ++_i25)
            {
              xfer += iprot->readI32(this->float_array[_i25]);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.float_array = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 19:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->double_array.clear();
            uint32_t _size26;
            ::apache::thrift::protocol::TType _etype29;
            xfer += iprot->readListBegin(_etype29, _size26);
            this->double_array.resize(_size26);
            uint32_t _i30;
            for (_i30 = 0; _i30 < _size26; ++_i30)
            {
              xfer += iprot->readDouble(this->double_array[_i30]);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.double_array = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 20:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->string_array.clear();
            uint32_t _size31;
            ::apache::thrift::protocol::TType _etype34;
            xfer += iprot->readListBegin(_etype34, _size31);
            this->string_array.resize(_size31);
            uint32_t _i35;
            for (_i35 = 0; _i35 < _size31; ++_i35)
            {
              xfer += iprot->readString(this->string_array[_i35]);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.string_array = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 21:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->byte_array_array.clear();
            uint32_t _size36;
            ::apache::thrift::protocol::TType _etype39;
            xfer += iprot->readListBegin(_etype39, _size36);
            this->byte_array_array.resize(_size36);
            uint32_t _i40;
            for (_i40 = 0; _i40 < _size36; ++_i40)
            {
              xfer += iprot->readBinary(this->byte_array_array[_i40]);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.byte_array_array = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 22:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          xfer += iprot->readI32(this->ref_val);
          this->__isset.ref_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 23:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->list_val.clear();
            uint32_t _size41;
            ::apache::thrift::protocol::TType _etype44;
            xfer += iprot->readListBegin(_etype44, _size41);
            this->list_val.resize(_size41);
            uint32_t _i45;
            for (_i45 = 0; _i45 < _size41; ++_i45)
            {
              xfer += iprot->readI32(this->list_val[_i45]);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.list_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 24:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readBinary(this->native_val);
          this->__isset.native_val = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 25:
        if (ftype == ::apache::thrift::protocol::T_BOOL) {
          xfer += iprot->readBool(this->null_val);
          this->__isset.null_val = true;
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

  return xfer;
}

uint32_t FieldValue::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("FieldValue");

  if (this->__isset.bool_val) {
    xfer += oprot->writeFieldBegin("bool_val", ::apache::thrift::protocol::T_BOOL, 1);
    xfer += oprot->writeBool(this->bool_val);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.byte_val) {
    xfer += oprot->writeFieldBegin("byte_val", ::apache::thrift::protocol::T_BYTE, 2);
    xfer += oprot->writeByte(this->byte_val);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.char_val) {
    xfer += oprot->writeFieldBegin("char_val", ::apache::thrift::protocol::T_I16, 3);
    xfer += oprot->writeI16(this->char_val);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.short_val) {
    xfer += oprot->writeFieldBegin("short_val", ::apache::thrift::protocol::T_I16, 4);
    xfer += oprot->writeI16(this->short_val);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.int_val) {
    xfer += oprot->writeFieldBegin("int_val", ::apache::thrift::protocol::T_I32, 5);
    xfer += oprot->writeI32(this->int_val);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.long_val) {
    xfer += oprot->writeFieldBegin("long_val", ::apache::thrift::protocol::T_I64, 6);
    xfer += oprot->writeI64(this->long_val);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.float_val) {
    xfer += oprot->writeFieldBegin("float_val", ::apache::thrift::protocol::T_I32, 7);
    xfer += oprot->writeI32(this->float_val);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.double_val) {
    xfer += oprot->writeFieldBegin("double_val", ::apache::thrift::protocol::T_DOUBLE, 8);
    xfer += oprot->writeDouble(this->double_val);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.string_val) {
    xfer += oprot->writeFieldBegin("string_val", ::apache::thrift::protocol::T_STRING, 9);
    xfer += oprot->writeString(this->string_val);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.decimal_val) {
    xfer += oprot->writeFieldBegin("decimal_val", ::apache::thrift::protocol::T_STRUCT, 10);
    xfer += this->decimal_val.write(oprot);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.timestamp_val) {
    xfer += oprot->writeFieldBegin("timestamp_val", ::apache::thrift::protocol::T_STRUCT, 11);
    xfer += this->timestamp_val.write(oprot);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.bool_array) {
    xfer += oprot->writeFieldBegin("bool_array", ::apache::thrift::protocol::T_LIST, 12);
    {
      xfer += oprot->writeListBegin(::apache::thrift::protocol::T_BOOL, static_cast<uint32_t>(this->bool_array.size()));
      std::vector<bool> ::const_iterator _iter46;
      for (_iter46 = this->bool_array.begin(); _iter46 != this->bool_array.end(); ++_iter46)
      {
        xfer += oprot->writeBool((*_iter46));
      }
      xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.byte_array) {
    xfer += oprot->writeFieldBegin("byte_array", ::apache::thrift::protocol::T_STRING, 13);
    xfer += oprot->writeBinary(this->byte_array);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.char_array) {
    xfer += oprot->writeFieldBegin("char_array", ::apache::thrift::protocol::T_STRING, 14);
    xfer += oprot->writeString(this->char_array);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.short_array) {
    xfer += oprot->writeFieldBegin("short_array", ::apache::thrift::protocol::T_LIST, 15);
    {
      xfer += oprot->writeListBegin(::apache::thrift::protocol::T_I16, static_cast<uint32_t>(this->short_array.size()));
      std::vector<int16_t> ::const_iterator _iter47;
      for (_iter47 = this->short_array.begin(); _iter47 != this->short_array.end(); ++_iter47)
      {
        xfer += oprot->writeI16((*_iter47));
      }
      xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.int_array) {
    xfer += oprot->writeFieldBegin("int_array", ::apache::thrift::protocol::T_LIST, 16);
    {
      xfer += oprot->writeListBegin(::apache::thrift::protocol::T_I32, static_cast<uint32_t>(this->int_array.size()));
      std::vector<int32_t> ::const_iterator _iter48;
      for (_iter48 = this->int_array.begin(); _iter48 != this->int_array.end(); ++_iter48)
      {
        xfer += oprot->writeI32((*_iter48));
      }
      xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.long_array) {
    xfer += oprot->writeFieldBegin("long_array", ::apache::thrift::protocol::T_LIST, 17);
    {
      xfer += oprot->writeListBegin(::apache::thrift::protocol::T_I64, static_cast<uint32_t>(this->long_array.size()));
      std::vector<int64_t> ::const_iterator _iter49;
      for (_iter49 = this->long_array.begin(); _iter49 != this->long_array.end(); ++_iter49)
      {
        xfer += oprot->writeI64((*_iter49));
      }
      xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.float_array) {
    xfer += oprot->writeFieldBegin("float_array", ::apache::thrift::protocol::T_LIST, 18);
    {
      xfer += oprot->writeListBegin(::apache::thrift::protocol::T_I32, static_cast<uint32_t>(this->float_array.size()));
      std::vector<int32_t> ::const_iterator _iter50;
      for (_iter50 = this->float_array.begin(); _iter50 != this->float_array.end(); ++_iter50)
      {
        xfer += oprot->writeI32((*_iter50));
      }
      xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.double_array) {
    xfer += oprot->writeFieldBegin("double_array", ::apache::thrift::protocol::T_LIST, 19);
    {
      xfer += oprot->writeListBegin(::apache::thrift::protocol::T_DOUBLE, static_cast<uint32_t>(this->double_array.size()));
      std::vector<double> ::const_iterator _iter51;
      for (_iter51 = this->double_array.begin(); _iter51 != this->double_array.end(); ++_iter51)
      {
        xfer += oprot->writeDouble((*_iter51));
      }
      xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.string_array) {
    xfer += oprot->writeFieldBegin("string_array", ::apache::thrift::protocol::T_LIST, 20);
    {
      xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRING, static_cast<uint32_t>(this->string_array.size()));
      std::vector<std::string> ::const_iterator _iter52;
      for (_iter52 = this->string_array.begin(); _iter52 != this->string_array.end(); ++_iter52)
      {
        xfer += oprot->writeString((*_iter52));
      }
      xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.byte_array_array) {
    xfer += oprot->writeFieldBegin("byte_array_array", ::apache::thrift::protocol::T_LIST, 21);
    {
      xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRING, static_cast<uint32_t>(this->byte_array_array.size()));
      std::vector<std::string> ::const_iterator _iter53;
      for (_iter53 = this->byte_array_array.begin(); _iter53 != this->byte_array_array.end(); ++_iter53)
      {
        xfer += oprot->writeBinary((*_iter53));
      }
      xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.ref_val) {
    xfer += oprot->writeFieldBegin("ref_val", ::apache::thrift::protocol::T_I32, 22);
    xfer += oprot->writeI32(this->ref_val);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.list_val) {
    xfer += oprot->writeFieldBegin("list_val", ::apache::thrift::protocol::T_LIST, 23);
    {
      xfer += oprot->writeListBegin(::apache::thrift::protocol::T_I32, static_cast<uint32_t>(this->list_val.size()));
      std::vector<int32_t> ::const_iterator _iter54;
      for (_iter54 = this->list_val.begin(); _iter54 != this->list_val.end(); ++_iter54)
      {
        xfer += oprot->writeI32((*_iter54));
      }
      xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.native_val) {
    xfer += oprot->writeFieldBegin("native_val", ::apache::thrift::protocol::T_STRING, 24);
    xfer += oprot->writeBinary(this->native_val);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.null_val) {
    xfer += oprot->writeFieldBegin("null_val", ::apache::thrift::protocol::T_BOOL, 25);
    xfer += oprot->writeBool(this->null_val);
    xfer += oprot->writeFieldEnd();
  }
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

void swap(FieldValue &a, FieldValue &b) {
  using ::std::swap;
  swap(a.bool_val, b.bool_val);
  swap(a.byte_val, b.byte_val);
  swap(a.char_val, b.char_val);
  swap(a.short_val, b.short_val);
  swap(a.int_val, b.int_val);
  swap(a.long_val, b.long_val);
  swap(a.float_val, b.float_val);
  swap(a.double_val, b.double_val);
  swap(a.string_val, b.string_val);
  swap(a.decimal_val, b.decimal_val);
  swap(a.timestamp_val, b.timestamp_val);
  swap(a.bool_array, b.bool_array);
  swap(a.byte_array, b.byte_array);
  swap(a.char_array, b.char_array);
  swap(a.short_array, b.short_array);
  swap(a.int_array, b.int_array);
  swap(a.long_array, b.long_array);
  swap(a.float_array, b.float_array);
  swap(a.double_array, b.double_array);
  swap(a.string_array, b.string_array);
  swap(a.byte_array_array, b.byte_array_array);
  swap(a.ref_val, b.ref_val);
  swap(a.list_val, b.list_val);
  swap(a.native_val, b.native_val);
  swap(a.null_val, b.null_val);
  swap(a.__isset, b.__isset);
}

}}}} // namespace