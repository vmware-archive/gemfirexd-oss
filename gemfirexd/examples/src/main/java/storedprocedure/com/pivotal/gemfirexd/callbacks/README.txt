The source of DBSynchronizer is provided as an example implementation of AsyncEventListener AS IS WITHOUT ANY WARRANTY.

A helper class AsyncEventHelper is also provided with source. It uses a few internal APIs for internal trace flags but otherwise uses public APIs.

Both classes are identical to those built into GemFireXD.

Users are free to changed and use the code in any way fit. However, the code of these classes should not be changed to override that built into GemFireXD since other parts of the product may depend on the two classes. Instead create new classes by either copying and modifying the code, or extending these two classes.
