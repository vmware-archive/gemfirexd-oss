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
package quickstart;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/**
 * Custom GoldenComparator which returns the string of the golden file name
 * as the output for simple unit testing of the quickstart testing framework.
 * 
 * @author Kirk Lund
 */
public class GoldenStringComparator extends RegexGoldenComparator {

  protected GoldenStringComparator(String[] expectedProblemLines) {
    super(expectedProblemLines);
  }
  
  protected Reader readGoldenFile(String goldenFileName) throws IOException {
    return new StringReader(goldenFileName);
  }
}
