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

#ifndef _AUTOPTR_H_
#define _AUTOPTR_H_


#include <vector>
#include <string>
#include <iostream> // included to standardize new and delete.
#include <iterator>
#include <assert.h>


/// @brief Provides pointer management. 
///
/// Wraps the owned pointer to guarantee its automatic deletion when the object goes out of
/// scope.
///
/// You can copy and assign between AutoPtr objects. The ownership is always passed along.
/// If trying to assign to a AutoPtr that already wraps a pointer, that pointer will be
/// deleted before the next one is assigned.
///
/// We will protect against using NULL contained pointers with asserts to catch programmer
/// errors. We will depend on callers using the new operator to throw exceptions on failure to
/// protect at runtime.
///
/// This object cannot be used in a container that requires that the objects have a copy
/// constructor and assignment operator that have a const parameter. This means that it wants
/// to make an exact copy. This is not the case with this object as the the AutoPtr
/// objects being used for the copy construction or assignment are changed. These AutoPtr
/// objects lose the pointers they contain when the other object is initialized with it.
///
/// Here are some guidelines for using the AutoPtr.
/// - Never create a AutoPtr pointer using new. This defeats its purpose.
/// - Never use the AutoPtr to wrap a pointer to an array since AutoPtr uses delete
///   and not delete [].
/// - If used properly, there is only ever one instance of a AutoPtr that contains a
///   particular pointer. There is no reference counting that would enable it to be a shared
///   pointer object.
/// - They can be passed around as parameters. If they are passed as a const reference you will
///   still be be able to access the contained pointer after the call, but only as type
///   const T*. The const will have the compiler prevent any of the non const methods from
///   being called on it. You could always trick it by doing something like
///   delete AutoPtr.Get() but this is improper usage.
/// - Do not instantiate AutoPtr on a const-qualified type (you might get indecipherable
///   compile errors).
template <typename T>
class AutoPtr
{
// Public ======================================================================================
public:

    /// @brief Default constructor.
    ///
    /// @param in_ptr   Pointer of explicit T type to initialize the owned pointer.
    explicit AutoPtr(T* in_ptr = NULL) : m_ownedObjPtr(in_ptr)
    {
        ; // Do nothing.
    }

    /// @brief Copy constructor.
    ///
    /// The AutoPtr object used to construct this object will give up ownership of its
    /// contained pointer to the object being constructed.
    ///
    /// This is not like a typical copy constructor since the other AutoPtr is modified.
    ///
    /// @param in_obj   Const reference to another object containing the same type of owned.
    ///                 object pointer. The other object will lose its owned pointer by passing
    ///                 ownership to this instance.
    AutoPtr(const AutoPtr<T>& in_obj) : 
        m_ownedObjPtr(const_cast< AutoPtr<T>& >(in_obj).Detach())
    {
        ; // Do nothing.
    }

    /// @brief Destructor.
    ///
    /// Automatically cleans up the owned pointer using operator delete.
    ~AutoPtr()
    {
        delete this->m_ownedObjPtr;
    }

    /// @brief Assignment operator.
    ///
    /// The receiving object will delete its own contained pointer and replace it with the one 
    /// being passed in.
    ///
    /// @param in_rhs   A pointer that this object will own.
    ///
    /// @return a reference to the object on the left-hand side of the assignment.
    AutoPtr& operator=(T* in_rhs)
    {
        this->Attach(in_rhs);
        return *this;
    }

    /// @brief Assignment operator.
    ///
    /// Note that the AutoPtr on the right hand hand of the assignment will give up
    /// ownership of its contained pointer. The receiving object will delete its own contained
    /// pointer and replace it with the one being passed in.
    ///
    /// This is not a normal assignment operator because the right-hand side is modified.
    ///
    /// @param in_rhs   A AutoPtr object to initialize this object with its contained pointer.
    ///
    /// @return a reference to the object on the left-hand side of the assignment.
    AutoPtr& operator=(const AutoPtr<T>& in_rhs)
    {
        this->Attach(const_cast< AutoPtr<T>& >(in_rhs).Detach());
        return *this;
    }

    /// @brief Return the owned pointer.
    ///
    /// This is used so that the AutoPtr can masquerade as the pointer it contains.
    ///
    /// @return the contained pointer. (NOT OWN)
    T* operator->()
    {
        // To avoid costly checking we will only catch programmer errors with an assert. We 
        // will depend on the ability of new to throw an exception on failure.
//            assert(!this->IsNull());
        return this->Get();
    }

    /// @brief Return the const owned pointer.
    ///
    /// This is used so that the AutoPtr can masquerade as the pointer it contains.
    ///
    /// @return the contained pointer, const-qualified. (NOT OWN)
    const T* operator->() const
    {
        // To avoid costly checking we will only catch programmer errors with an assert. We 
        // will depend on the ability of new to throw an exception on failure.
//            assert(!this->IsNull());
        return this->Get();
    }

    /// @brief Return a reference to the object pointed to by the owned pointer.
    ///
    /// @warning De-referencing a NULL pointer will cause a segfault in Unix. Check the 
    /// validity of the pointer by calling AutoPtr::IsNull() before de-referencing.
    ///
    /// @return     A reference to the object pointed to by the contained pointer.
    T& operator*()
    {
        // To avoid costly checking we will only catch programmer errors with an assert. We 
        // will depend on the ability of new to throw an exception on failure.
//            assert(!this->IsNull());
        return *(this->Get());
    }

    /// @brief Return a const reference to the object pointed to by the owned pointer.
    ///
    /// @warning De-referencing a NULL pointer will cause a segfault in Unix. Check the validity
    /// of the pointer by calling AutoPtr::IsNull() before dereferencing.
    ///
    /// @return     A reference to the object pointed to by the contained pointer, const-qualified.
    const T& operator*() const
    {
        // To avoid costly checking we will only catch programmer errors with an assert. We 
        // will depend on the ability of new to throw an exception on failure.
        assert(m_ownedObjPtr != NULL);
        return *(this->Get());
    }

    /// @brief Gives access to the contained pointer.
    ///
    /// @warning This method does not pass off ownership of the contained pointer.
    ///
    /// @return the contained pointer. (NOT OWN)
    T* Get()
    {
        return this->m_ownedObjPtr;
    }

    /// @brief Gives const access to the contained pointer.
    ///
    /// @warning This method does not pass off ownership of the contained pointer.
    ///
    /// @return the contained pointer, const-qualified. (NOT OWN)
    const T* Get() const
    {
        return this->m_ownedObjPtr;
    }

    /// @brief Replaces the contained pointer with the one passed in.
    ///
    /// If the pointer being passed in is not the same as the contained pointer, then the
    /// contained pointer is deleted and the new pointer takes its place.
    ///
    /// @param in_ptr   Pointer to replace the contained pointer.
    void Attach(T* in_ptr)
    {
        if (in_ptr != this->m_ownedObjPtr)
        {
            // It is safe to delete a NULL pointer.  There is no need to check.
            delete this->m_ownedObjPtr;
            this->m_ownedObjPtr = in_ptr;
        }
    }

    /// @brief Releases ownership of the pointer and sets it to NULL.
    ///
    /// The responsibility for managing and deleting the pointer is transferred to the caller
    /// of this method. The object retains no knowledge of its former contained pointer.
    ///
    /// @return the previously contained pointer. (OWN)
    T* Detach()
    {
        T* tmpPtr = this->Get();
        this->m_ownedObjPtr = NULL;
        return tmpPtr;
    }

    /// @brief Check if the contained pointer is NULL.
    ///
    /// This will only check if the pointer is NULL or not. It would still be possible to fool
    /// the object by passing in a bogus pointer.
    ///
    /// @return     true if NULL, otherwise false.
    bool IsNull() const
    {
        return (NULL == this->m_ownedObjPtr);
    }

// Private =====================================================================================
private:
    /// @brief The pointer that is being wrapped.
    T* m_ownedObjPtr;
};


#endif
