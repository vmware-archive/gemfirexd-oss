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
package objects;

import hydra.GsRandom;
import java.util.*;
import java.io.Serializable;

/**
 * Represents an asset held in {@link AssetAccount}.
 */
public class Asset implements ConfigurableObject, Serializable {

  private static final GsRandom rng = new GsRandom(12); // need determinism

  private int assetId;
  private double value;

  public Asset() {
  }

  public void init(int anAssetId) {
    this.assetId = anAssetId;
    this.value = rng.nextDouble(1, AssetPrms.getMaxValue());
  }

  /**
   * Returns the id of the asset.
   */
  public int getAssetId(){
    return this.assetId;
  }

  /**
   * Returns the asset value.
   */
  public double getValue(){
    return this.value;
  }

  /**
   * Sets the asset value.
   */
  public void setValue(double d) {
    this.value = d;
  }

  public int getIndex() {
    return this.assetId;
  }

  public void validate(int index) {
    int encodedIndex = this.getIndex();
    if (encodedIndex != index) {
      String s = "Expected index " + index + ", got " + encodedIndex;
      throw new ObjectValidationException(s);
    }
  }

  public String toString(){
    return "Asset [assetId=" + this.assetId + " value=" + this.value + "]";
  }

  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj instanceof Asset) {
      Asset asset = (Asset)obj;
      return this.assetId == asset.assetId;
    } else {
      return false;
    }
  }

  public int hashCode() {
    return this.assetId;
  }
}
