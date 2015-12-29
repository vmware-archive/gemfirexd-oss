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
 * AutoPtr.h
 *
 *      Author: swale
 */

#ifndef AUTOPTR_H_
#define AUTOPTR_H_

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {

      /**
       * This is a smart pointer for strict ownership semantics having no
       * reference counting much like std::auto_ptr. The difference with
       * std::auto_ptr is that get(), dereference and other operators will
       * continue to work fine even after ownership has been given up by
       * an explicit call to release(). This helps keep a uniform interface
       * for managed and unmanaged pointers.
       *
       * @see std::auto_ptr
       */
      template<typename T>
      class AutoPtr
      {
      private:
        T* m_pOrig;
        T* m_p;

      public:
        /**
         * @brief Constructor for AutoPtr from a raw pointer.
         *
         * @param p the raw pointer pointer; this object will own
         *          the object pointed to by p
         */
        explicit AutoPtr(T* p = 0) throw () :
            m_pOrig(p), m_p(p) {
        }

        /**
         * @brief Constructor for AutoPtr from a raw pointer.
         *
         * @param p the raw pointer pointer; this object will own
         *          the object pointed to by p
         * @param manage if false then don't manage the pointer
         *          i.e. same affect as a release()
         */
        explicit AutoPtr(T* p, bool manage) throw () :
            m_pOrig(p), m_p(!manage ? 0 : p) {
        }

        /**
         * @brief Copy constructor for another AutoPtr of same type.
         *
         * @param o AutoPtr of the same type; the ownership of raw
         *          pointer will be transferred from o to this object
         */
        AutoPtr(const AutoPtr& o) throw () :
            m_pOrig(o.m_pOrig), m_p(o.m_p) {
          const_cast<AutoPtr&>(o).m_p = 0;
        }

        /**
         * @brief Copy constructor for another AutoPtr of different type.
         *
         * @param o AutoPtr of a different but "assignable" type;
         *          a pointer to T1 must be convertible to a pointer to T;
         *          the ownership of raw pointer will be transferred
         *          from o to this object
         */
        template<typename T1>
        AutoPtr(const AutoPtr<T1>& o) throw () :
            m_pOrig(o.m_pOrig), m_p(o.m_p) {
          const_cast<AutoPtr<T1>&>(o).m_p = 0;
        }

        /**
         * @brief Assignment operator for another AutoPtr of same type.
         *
         * @param o AutoPtr of the same type; the ownership of raw
         *          pointer will be transferred from o to this object while
         *          any pointer currently managed by this object is deleted
         */
        AutoPtr& operator=(const AutoPtr& o) throw () {
          if (m_p != o.m_p) {
            delete m_p;
            m_p = o.m_p;
            const_cast<AutoPtr&>(o).m_p = 0;
          }
          m_pOrig = o.m_pOrig;
          return *this;
        }

        /**
         * @brief Assignment operator for another AutoPtr of different type.
         *
         * @param o AutoPtr of a different but "assignable" type;
         *          a pointer to T1 must be convertible to a pointer to T;
         *          the ownership of raw pointer will be transferred
         *          from o to this object while any pointer currently managed
         *          by this object is deleted
         */
        template<typename T1>
        AutoPtr& operator=(const AutoPtr<T1>& o) throw () {
          const bool otherOwner = o.isOwner();
          reset(const_cast<AutoPtr<T1>&>(o).release());
          // don't take ownership if other one does not have it
          if (!otherOwner) {
            m_p = 0;
          }
          return *this;
        }

        /**
         * @brief Deference operator for the raw pointer. Unlike the
         * standard std::auto_ptr, this will still return the previously
         * assignied pointer after a release(), so that it works uniformly
         * before and after a release().
         */
        T& operator*() const throw () {
          return *m_pOrig;
        }

        /**
         * @brief Deference operator for the raw pointer. Unlike the
         * standard std::auto_ptr, this will still return the previously
         * assignied pointer after a release(), so that it works uniformly
         * before and after a release().
         */
        T* operator->() const throw () {
          return m_pOrig;
        }

        /**
         * @brief Return the raw pointer being managed. This AutoPtr can still
         * own the memory so caller should never delete the returned pointer.
         */
        inline T* get() throw () {
          return m_pOrig;
        }

        /**
         * @brief Return the raw pointer being managed. This AutoPtr can still
         * own the memory so caller should never delete the returned pointer.
         */
        inline const T* get() const throw () {
          return m_pOrig;
        }

        /**
         * @brief Return true if this object owns the raw object
         * as returned by get(), and false if ownership has been
         * relinquished by calling release().
         */
        inline bool isOwner() const throw () {
          return m_p;
        }

        /**
         * @brief Return true if the pointer held by this AutoPtr is NULL.
         * This will be false if release() has been invoked.
         */
        inline bool isNull() const throw () {
          return (m_p == 0);
        }

        /**
         * @brief Release ownership of the raw pointer, so when this object
         * goes out of scope it will no longer try to delete the raw pointer.
         * Unlike std::auto_ptr, dereference, get() and other operators will
         * still continue to return the previously managed pointer.
         *
         * @return the raw pointer that was being managed
         */
        T* release() throw () {
          m_p = 0;
          return m_pOrig;
        }

        /**
         * @brief Assume ownership of given pointer releasing ownership of
         * currently managed pointer, if any. If this object was owning
         * a pointer previously, then it is deleted.
         *
         * @param p the new raw pointer to be managed
         */
        void reset(T* p = NULL) throw () {
          if (p != m_p) {
            delete m_p;
            m_p = m_pOrig = p;
          }
        }

        /**
         * @brief Destructor that will delete the raw object being managed
         * by this AutoPtr. If the underlying object was relinquished
         * by a previous call to release() then this has no affect.
         */
        ~AutoPtr() {
          delete m_p;
        }
      };

    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* AUTOPTR_H_ */
