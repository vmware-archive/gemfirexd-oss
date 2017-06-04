/**
 * Autogenerated by Thrift Compiler (0.10.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */

#ifndef SNAPPYDATA_STRUCT_PREPARERESULT_H
#define SNAPPYDATA_STRUCT_PREPARERESULT_H


#include "snappydata_struct_Decimal.h"
#include "snappydata_struct_BlobChunk.h"
#include "snappydata_struct_ClobChunk.h"
#include "snappydata_struct_TransactionXid.h"
#include "snappydata_struct_ServiceMetaData.h"
#include "snappydata_struct_ServiceMetaDataArgs.h"
#include "snappydata_struct_OpenConnectionArgs.h"
#include "snappydata_struct_ConnectionProperties.h"
#include "snappydata_struct_HostAddress.h"
#include "snappydata_struct_SnappyExceptionData.h"
#include "snappydata_struct_StatementAttrs.h"
#include "snappydata_struct_ColumnValue.h"
#include "snappydata_struct_ColumnDescriptor.h"
#include "snappydata_struct_Row.h"
#include "snappydata_struct_OutputParameter.h"
#include "snappydata_struct_RowSet.h"

#include "snappydata_types.h"

namespace io { namespace snappydata { namespace thrift {

typedef struct _PrepareResult__isset {
  _PrepareResult__isset() : resultSetMetaData(false), warnings(false) {}
  bool resultSetMetaData :1;
  bool warnings :1;
} _PrepareResult__isset;

class PrepareResult {
 public:

  PrepareResult(const PrepareResult&);
  PrepareResult(PrepareResult&&) noexcept;
  PrepareResult& operator=(const PrepareResult&);
  PrepareResult& operator=(PrepareResult&&) noexcept;
  PrepareResult() : statementId(0), statementType(0) {
  }

  virtual ~PrepareResult() noexcept;
  int64_t statementId;
  int8_t statementType;
  std::vector<ColumnDescriptor>  parameterMetaData;
  std::vector<ColumnDescriptor>  resultSetMetaData;
  SnappyExceptionData warnings;

  _PrepareResult__isset __isset;

  void __set_statementId(const int64_t val);

  void __set_statementType(const int8_t val);

  void __set_parameterMetaData(const std::vector<ColumnDescriptor> & val);

  void __set_resultSetMetaData(const std::vector<ColumnDescriptor> & val);

  void __set_warnings(const SnappyExceptionData& val);

  bool operator == (const PrepareResult & rhs) const
  {
    if (!(statementId == rhs.statementId))
      return false;
    if (!(statementType == rhs.statementType))
      return false;
    if (!(parameterMetaData == rhs.parameterMetaData))
      return false;
    if (__isset.resultSetMetaData != rhs.__isset.resultSetMetaData)
      return false;
    else if (__isset.resultSetMetaData && !(resultSetMetaData == rhs.resultSetMetaData))
      return false;
    if (__isset.warnings != rhs.__isset.warnings)
      return false;
    else if (__isset.warnings && !(warnings == rhs.warnings))
      return false;
    return true;
  }
  bool operator != (const PrepareResult &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const PrepareResult & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

  virtual void printTo(std::ostream& out) const;
};

void swap(PrepareResult &a, PrepareResult &b) noexcept;

inline std::ostream& operator<<(std::ostream& out, const PrepareResult& obj)
{
  obj.printTo(out);
  return out;
}

}}} // namespace

#endif