/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package resumeTx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.internal.cache.TXId;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gemfire.internal.util.BlobHelper;

public class RtxUtilVersionHelper {

  public static Map<TXId, TxInfo> convertWrapperMapToActiveTxMap(
      Map<ActiveTxWrapper, TxInfoWrapper> wrapperMap)
      throws ClassNotFoundException, IOException {
    Map<TXId, TxInfo> activeTxnMap = new HashMap<TXId, TxInfo>();
    for (Object obj : wrapperMap.keySet()) {
      ActiveTxWrapper wrapper = (ActiveTxWrapper) obj;
      TxInfoWrapper txInfoWrapper = (TxInfoWrapper) wrapperMap.get(wrapper);
      TXId txId = null;
      txId = (TXId) BlobHelper.deserializeBlob(wrapper.serializedTXId, Version.GFE_70, null);
      TxInfo txInfo = (TxInfo) BlobHelper.deserializeBlob(
          txInfoWrapper.getTxInfoByteArray(), Version.GFE_70, null);
      activeTxnMap.put(txId, txInfo);
    }
    return activeTxnMap;
  }

  public static Map<ActiveTxWrapper, TxInfoWrapper> convertActiveTxMapToWrapperMap(
      Map<TXId, TxInfo> activeTxnMap)
      throws ClassNotFoundException, IOException {
    Map<ActiveTxWrapper, TxInfoWrapper> wrapperMap = new HashMap<ActiveTxWrapper, TxInfoWrapper>();
    for (Object obj : activeTxnMap.keySet()) {
      TXId txId = (TXId) obj;
      TxInfo txInfo = (TxInfo) activeTxnMap.get(txId);
      byte[] serializedTXId = BlobHelper.serializeToBlob(txId, Version.GFE_70);
      ActiveTxWrapper wrapper = ActiveTxWrapper.getWrappedActiveTx(serializedTXId);
      byte[] serializedTxInfo = BlobHelper.serializeToBlob(txInfo, Version.GFE_70);
      TxInfoWrapper txInfoWrapper = TxInfoWrapper.getWrappedTxInfo(serializedTxInfo);

      wrapperMap.put(wrapper, txInfoWrapper);
    }
    return wrapperMap;
  }
  
  public static List<byte[]> convertTransactionIDListToByteArrayList(List<TransactionId> txIdList) throws Exception {
    List<byte[]> list = Collections.synchronizedList(new ArrayList<byte[]>());
    for (int i = 0; i < txIdList.size(); i++) {
      list.add(BlobHelper.serializeToBlob(txIdList.get(i), Version.GFE_70));
    }
    return list;
  }
  
  public static List<TransactionId> convertByteArrayListToTransactionIDList(List<byte[]> list) throws Exception {
    List<TransactionId> txIdList = Collections.synchronizedList(new ArrayList<TransactionId>());
    for (int i = 0; i < list.size(); i++) {
      txIdList.add((TransactionId)BlobHelper.deserializeBlob(list.get(i), Version.GFE_70, null));
    }
    return txIdList;
  }
  
}
