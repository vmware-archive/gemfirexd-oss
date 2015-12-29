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
package com.gemstone.gemfire.internal;

import java.io.File;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * SimpleStatSampler is a functional implementation of HostStatSampler
 * that samples statistics stored in local java memory and does not
 * require any native code or additional GemFire features.
 * <p>
 * The StatisticsManager may be implemented by LocalStatisticsFactory and does
 * not require a GemFire connection.

 * @author Darrel Schneider
 * @author Kirk Lund
 */
public class SimpleStatSampler extends HostStatSampler  {
  
  public static final String ARCHIVE_FILE_NAME_PROPERTY = "stats.archive-file";
  public static final String FILE_SIZE_LIMIT_PROPERTY = "stats.file-size-limit";
  public static final String DISK_SPACE_LIMIT_PROPERTY = "stats.disk-space-limit";
  public static final String SAMPLE_RATE_PROPERTY = "stats.sample-rate";
  
  public static final String DEFAULT_ARCHIVE_FILE_NAME = "stats.gfs";
  public static final long DEFAULT_FILE_SIZE_LIMIT = 0;
  public static final long DEFAULT_DISK_SPACE_LIMIT = 0;
  public static final int DEFAULT_SAMPLE_RATE = 1000;
  
  private final File archiveFileName = new File(System.getProperty(ARCHIVE_FILE_NAME_PROPERTY, DEFAULT_ARCHIVE_FILE_NAME));
  private final long archiveFileSizeLimit = Long.getLong(FILE_SIZE_LIMIT_PROPERTY, DEFAULT_FILE_SIZE_LIMIT).longValue() * (1024*1024);
  private final long archiveDiskSpaceLimit = Long.getLong(DISK_SPACE_LIMIT_PROPERTY, DEFAULT_DISK_SPACE_LIMIT).longValue() * (1024*1024);
  private final int sampleRate = Integer.getInteger(SAMPLE_RATE_PROPERTY, DEFAULT_SAMPLE_RATE).intValue();

  private final StatisticsManager sm;

  public SimpleStatSampler(CancelCriterion stopper, StatisticsManager sm) {
    super(stopper, sm.getLogWriterI18n(), new StatSamplerStats(sm, sm.getId()));
    this.sm = sm;
    sm.getLogWriterI18n().config(LocalizedStrings.SimpleStatSampler_STATSSAMPLERATE_0, getSampleRate());
  }

  @Override
  protected void checkListeners() {
    // do nothing
  }
  
  @Override
  public File getArchiveFileName() {
    return this.archiveFileName;
  }
  
  @Override
  public long getArchiveFileSizeLimit() {
    if (fileSizeLimitInKB()) {
      return this.archiveFileSizeLimit / 1024;
    } else {
      return this.archiveFileSizeLimit;
    }
  }
  
  @Override
  public long getArchiveDiskSpaceLimit() {
    if (fileSizeLimitInKB()) {
      return this.archiveDiskSpaceLimit / 1024;
    } else {
      return this.archiveDiskSpaceLimit;
    }
  }
  
  @Override
  public String getProductDescription() {
    return "Unknown product";
  }

  @Override
  protected StatisticsManager getStatisticsManager() {
    return this.sm;
  }
  
  @Override
  protected int getSampleRate() {
    return this.sampleRate;
  }
  
  @Override
  public boolean isSamplingEnabled() {
    return true;
  }
}
