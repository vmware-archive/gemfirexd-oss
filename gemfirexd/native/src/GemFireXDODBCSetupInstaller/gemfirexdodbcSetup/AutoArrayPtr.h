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

#ifndef _AUTOARRAYPTR_H_
#define _AUTOARRAYPTR_H_


#include <vector>
#include <string>
#include <iostream> // included to standardize new and delete.
#include <iterator>
#include <assert.h>


/// @brief Provides pointer management. 
///
/// Wraps the owned array pointer to guarantee its automatic deletion when the object goes out 
/// of scope. Note that this is NOT for an array of pointers, but for a pointer to an array.
///
/// You can copy and assign between AutoArrayPtr objects. The ownership is always passed along.
/// If trying to assign to a AutoArrayPtr that already wraps an array pointer, that pointer will
/// be deleted before the next one is assigned.
///
/// We will protect against using NULL contained array pointers with asserts to catch programmer
/// errors. We will depend on callers using the new [] operator to throw exceptions on failure 
/// to protect at runtime.
///
/// This object cannot be used in a container that requires that the objects have a copy
/// constructor and assignment operator that have a const parameter. This means that it wants
/// to make an exact copy. This is not the case with this object as the the AutoArrayPtr
/// objects being used for the copy construction or assignment are changed. These AutoArrayPtr
/// objects lose the pointers they contain when the other object is initialized with it.
///
/// Here are some guidelines for using the AutoArrayPtr.
/// - Never create a AutoArrayPtr pointer using new. This defeats its purpose.
/// - Never use the AutoArrayPtr to wrap a pointer since AutoArrayPtr uses delete [] and not
///   delete.
/// - If used properly, there is only ever one instance of a AutoArrayPtr that contains a
///   particular array pointer. There is no reference counting that would enable it to be a 
///   shared pointer object.
/// - They can be passed around as parameters. If they are passed as a const reference you will
///   still be be able to access the contained array pointer after the call, but only as type
///   const T*. The const will have the compiler prevent any of the non const methods from
///   being called on it. You could always trick it by doing something like
///   delete AutoArrayPtr.Get() but this is improper usage.
/// - Do not instantiate AutoArrayPtr on a const-qualified type (you might get indecipherable
///   compile errors).
template <typename T>
class AutoArrayPtr
{
// Public ======================================================================================
public:
    /// @brief Default constructor.
    ///
    /// @param in_ptr       Pointer of explicit T type to initialize the owned pointer. (OWN)
    /// @param in_length    The length of the buffer to allocate.
    explicit AutoArrayPtr(T* in_ptr = NULL, size_t in_length = 0) : 
        m_length(in_length),
        m_ownedObjPtr(in_ptr)
    {
        ; // Do nothing.
    }

    /// @brief Constructor which allocates a new array with new T[].
    ///
    /// This can only be used if T has a public default constructor.
    ///
    /// @param in_length    Length of the buffer to allocate (number of elements of T, not size 
    ///                     in bytes)
    AutoArrayPtr(size_t in_length) : 
        m_length(in_length),
        m_ownedObjPtr(new T[in_length])
    {
        ; // Do nothing.
    }

    /// @brief Copy constructor.
    ///
    /// The AutoArrayPtr object used to construct this object will give up ownership of its
    /// contained pointer to the object being constructed.
    ///
    /// This is not like a typical copy constructor since the other AutoArrayPtr is modified.
    ///
    /// @param in_obj       Const reference to another object containing the same type of owned
    ///                     object pointer. The other object will lose its owned pointer by 
    ///                     passing ownership to this instance.
    AutoArrayPtr(const AutoArrayPtr<T>& in_obj) : 
        m_length(in_obj.m_length), //m_length must be initialized first or the Detach() will zero it.
        m_ownedObjPtr(const_cast< AutoArrayPtr<T>& >(in_obj).Detach())
    {
        ; // Do nothing.
    }

    /// @brief Destructor.
    ///
    /// Automatically cleans up the owned pointer using operator delete.
    ~AutoArrayPtr()
    {
        delete [] this->m_ownedObjPtr;
    }

    /// @brief Assignment operator.
    ///
    /// The receiving object will delete its own contained pointer and replace it with the one 
    /// being passed in.
    /// Note: This sets the length to 0 because it can't be known with this operator.
    ///
    /// @param in_rhs       A pointer that this object will own.
    ///
    /// @return a reference to the object on the left-hand side of the assignment.
    AutoArrayPtr& operator=(T* in_rhs)
    {
        this->Attach(in_rhs, 0);
        return *this;
    }

    /// @brief Assignment operator.
    ///
    /// Note that the AutoArrayPtr on the right hand hand of the assignment will give up
    /// ownership of its contained pointer. The receiving object will delete its own contained
    /// pointer and replace it with the one being passed in.
    ///
    /// This is not a normal assignment operator because the right-hand side is modified.
    ///
    /// @param in_rhs       A AutoArrayPtr object to initialize this object with its contained 
    ///                     pointer.
    ///
    /// @return a reference to the object on the left-hand side of the assignment.
    AutoArrayPtr& operator=(const AutoArrayPtr<T>& in_rhs)
    {
        // Get the length first because the Detach below will zero in_rhs.Length()
        size_t rhsLength = in_rhs.GetLength();
        this->Attach(const_cast< AutoArrayPtr<T>& >(in_rhs).Detach(), rhsLength);
        return *this;
    }

    /// @brief Return a reference to the array item at the specified index.
    ///
    /// @param in_index     The index into the array.
    ///
    /// @return A reference to the array item at the specified index.
    T& operator[](size_t in_index)
    {
        assert(m_ownedObjPtr != NULL);
        return m_ownedObjPtr[in_index];
    }

    /// @brief Return a const reference to the array item at the specified index.
    ///
    /// @param in_index     The index into the array.
    ///
    /// @return A const reference to the array item at the specified index.
    T& operator[](size_t in_index) const
    {
        assert(m_ownedObjPtr != NULL);
        return m_ownedObjPtr[in_index];
    }

    /// @brief Return a reference to the object pointed to by the owned pointer.
    ///
    /// @warning De-referencing a NULL pointer will cause a segfault in Unix. Check the 
    /// validity of the pointer by calling AutoArrayPtr::IsNull() before de-referencing.
    ///
    /// @return A reference to the object pointed to by the contained pointer.
    T& operator*()
    {
        // To avoid costly checking we will only catch programmer errors with an assert. We 
        // will depend on the ability of new to throw an exception on failure.
        assert(m_ownedObjPtr != NULL);
        return *(this->Get());
    }

    /// @brief Return a const reference to the object pointed to by the owned pointer.
    ///
    /// @warning De-referencing a NULL pointer will cause a segfault in Unix. Check the validity
    /// of the pointer by calling AutoArrayPtr::IsNull() before dereferencing.
    ///
    /// @return A reference to the object pointed to by the contained pointer, const-qualified.
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

    /// @brief Gets the length of the array (count of elements of T)
    ///        Note: Depending on the context an AutoArrayPtr is used in, the length 
    ///        may not be set (default to 0), or be unknown (eg. nul-terminated string).
    ///
    /// @return The length of the array
    size_t GetLength() const
    {
        return m_length;
    }

    /// @brief Replaces the contained pointer with the one passed in.
    ///
    /// If the pointer being passed in is not the same as the contained pointer, then the
    /// contained pointer is deleted and the new pointer takes its place.
    ///
    /// @param in_ptr       Pointer to replace the contained pointer. (OWN)
    /// @param in_length    The length of the passed in buffer.
    void Attach(T* in_ptr, size_t in_length)
    {
        if (in_ptr != this->m_ownedObjPtr)
        {
            // It is safe to delete a NULL pointer.  There is no need to check.
            delete [] this->m_ownedObjPtr;
            this->m_ownedObjPtr = in_ptr;
            this->m_length = in_length;
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
        this->m_length = 0;
        return tmpPtr;
    }

    /// @brief Check if the contained pointer is NULL.
    ///
    /// This will only check if the pointer is NULL or not. It would still be possible to fool
    /// the object by passing in a bogus pointer.
    ///
    /// @return true if NULL, otherwise false.
    inline bool IsNull() const {
        return (this->m_ownedObjPtr == NULL);
    }

// Private =====================================================================================
private:
    /// @brief The length of the array (count of elements of T)
    size_t m_length;

    /// @brief The pointer that is being wrapped. (OWN)
    T* m_ownedObjPtr;
};


#endif
