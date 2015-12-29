///////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2001, Eric D. Friedman All Rights Reserved.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
///////////////////////////////////////////////////////////////////////////////
/*
 * Contains changes for GemFireXD distributed data platform.
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package com.gemstone.gnu.trove.benchmark;

import java.util.*;
import com.gemstone.gnu.trove.*;

public class MemoryUsage {
    public static long sizeOf(Creator c) {
        long size= 0;
        Object[] objects = new Object[100];
        try {
            /* Object primer = GemStoneAddition */ c.create();
            long startingMemoryUse = getUsedMemory();
            for (int i = 0; i < objects.length; i++) {
                objects[i] = c.create();
            }
            long endingMemoryUse = getUsedMemory();
            float approxSize = (endingMemoryUse - 
                                startingMemoryUse) / 100f;
            size = Math.round(approxSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return size;
    }

    private static long getUsedMemory() {
        gc();
        long totalMemory = Runtime.getRuntime().totalMemory();
        gc();
        long freeMemory = Runtime.getRuntime().freeMemory();
        long usedMemory = totalMemory - freeMemory;
        return usedMemory;
    }

    protected static void gc() {
        try {
            System.gc();
            Thread.sleep(100);
            System.runFinalization();
            Thread.sleep(100);
            System.gc();
            Thread.sleep(100);
            System.runFinalization();
            Thread.sleep(100);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            MemoryComparator set =
                new MemoryComparator(new TroveSetCreator(),
                                     new JavasoftSetCreator(),
                                     "Compare size of Set implementation: 1,000 Integer objects measured in bytes");
            set.compare();
            set = null;
            MemoryComparator list =
                new MemoryComparator(new TroveListCreator(),
                                     new JavasoftListCreator(),
                                     "Compare size of LinkedList implementation: 1,000 TLinkableAdaptor objects measured in bytes");
            list.compare();
            list = null;
            MemoryComparator list2 =
                new MemoryComparator(new TroveIntArrayListCreator(),
                                     new JavasoftIntegerArrayListCreator(),
                                     "Compare size of int/IntegerArrayList implementation: 1,000 ints measured in bytes");
            list2.compare();
            list2 = null;

            MemoryComparator map =
                new MemoryComparator(new TroveMapCreator(),
                                     new JavasoftMapCreator(),
                                     "Compare size of Map implementation: 1,000 Integer->Integer mappings measured in bytes");
            map.compare();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    static class MemoryComparator {
        Creator trove, javasoft;
        String description;
        MemoryComparator(Creator trove,
                         Creator javasoft,
                         String description) {
            this.trove = trove;
            this.javasoft = javasoft;
            this.description = description;
        }

        public void compare() {
            gc();
            long j = sizeOf(javasoft);
            gc();
            long t = sizeOf(trove);

            long p = Math.round((double)/*GemStoneAddition*/t * 100 / j * 100) / 100;

            System.out.println("--------------------------");
            System.out.println(description);
            System.out.println("javasoft: " + j);
            System.out.println("trove: " + t);
            System.out.println("trove's collection requires " + p + "% of the memory needed by javasoft's collection");
        }
    }

    interface Creator {
        Object create();
    }

    static class TroveIntArrayListCreator implements Creator {
        public Object create() {
            TIntArrayList list = new TIntArrayList();
            for (int i = 0; i < 1000; i++) {
                list.add(i);
            }
            list.trimToSize();
            return list;
        }
    }

    static class JavasoftIntegerArrayListCreator implements Creator {
        public Object create() {
            ArrayList list = new ArrayList();
            for (int i = 0; i < 1000; i++) {
                Integer x = Integer.valueOf(i);
                list.add(x);
            }
            list.trimToSize();
            return list;
        }
    }

    static class TroveMapCreator implements Creator {
        public Object create() {
            THashMap map = new THashMap();
            for (int i = 0; i < 1000; i++) {
                Integer x = Integer.valueOf(i);
                map.put(x,x);
            }
            return map;
        }
    }

    static class JavasoftMapCreator implements Creator {
        public Object create() {
            HashMap map = new HashMap();
            for (int i = 0; i < 1000; i++) {
                Integer x = Integer.valueOf(i);
                map.put(x,x);
            }
            return map;
        }
    }

    static class TroveSetCreator implements Creator {
        public Object create() {
            THashSet map = new THashSet();
            for (int i = 0; i < 1000; i++) {
                Integer x = Integer.valueOf(i);
                map.add(x);
            }
            return map;
        }
    }

    static class JavasoftSetCreator implements Creator {
        public Object create() {
            HashSet map = new HashSet();
            for (int i = 0; i < 1000; i++) {
                Integer x = Integer.valueOf(i);
                map.add(x);
            }
            return map;
        }
    }

    static class TroveListCreator implements Creator {
        public Object create() {
            TLinkedList list = new TLinkedList();
            for (int i = 0; i < 1000; i++) {
                TLinkableAdaptor a = new TLinkableAdaptor();
                list.add(a);
            }
            return list;
        }
    }

    static class JavasoftListCreator implements Creator {
        public Object create() {
            LinkedList list = new LinkedList();
            for (int i = 0; i < 1000; i++) {
                TLinkableAdaptor a = new TLinkableAdaptor();
                list.add(a);
            }
            return list;
        }
    }
}
