/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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
package com.gemstone.org.jgroups.util;

/**
 * This class forms the basis of the i18n strategy. Its primary function is to
 * be used as a key to be passed to an instance of StringIdResourceBundle.
 * @author kbanks
 * @since 6.0 
 */
public interface StringId {
  /**
   * Accessor for the raw (unformatted) text of this StringId
   * @return unformated text
   **/ 
  public String getRawText();
  
  /**
   * @return the English translation of this StringId
   **/ 
  @Override
  public String toString();


  /**
   * Substitutes parameter Objects into the text
   * @see java.text.MessageFormat
   * @return the English translation of this StringId
   **/ 
  public String toString(Object ... params);

  /**
   * @return the translation of this StringId based on the current {@link java.util.Locale}
   **/ 
  public String toLocalizedString();
  
  /**
   * Substitutes parameter Objects into the text
   * @see java.text.MessageFormat
   * @return the translation of this StringId based on the current {@link java.util.Locale}
   **/ 
  public String toLocalizedString(Object ... params);
}
