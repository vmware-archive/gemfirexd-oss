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

/**
 *  Created on: 4 Jun 2014
 *      Author: swale
 */

#ifndef MESSAGEBASE_H_
#define MESSAGEBASE_H_

#include "common/Base.h"

#include <exception>
#include <vector>

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {

      namespace impl
      {
        /**
         * Forward declaration for class that can update the message parts
         * of MessageBases at runtime as per locale.
         */
        class MessageRegistry;
      }

      class MessageBase
      {
      private:
        // no copy constructor or assignment operator
        MessageBase(const MessageBase&);
        MessageBase& operator=(const MessageBase&);

      protected:
        std::string m_messageId;
        std::vector<std::string> m_messageParts;

        MessageBase();
        virtual ~MessageBase();

        void initialize(const char* messageId);
        void initialize(const std::string& messageId);

        void addMessagePart(const char* messagePart);

      public:
        const std::string& getMessageId() const throw ()
        {
          return m_messageId;
        }
        size_t getNumMessageParts() const throw ();
        const std::string& getMessagePart(int index) const;

        friend class impl::MessageRegistry;
      };

      class MessageException : public std::exception
      {
      private:
        const std::string m_message;

      public:
        MessageException(const char* message) throw () :
            m_message(message)
        {
        }

        virtual ~MessageException() throw ();

        virtual const char* what() const throw ();
      };

    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* MESSAGEBASE_H_ */
