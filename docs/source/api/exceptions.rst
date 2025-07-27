Exceptions
=========

.. module:: proto_db.exceptions

This module provides the exception classes used by ProtoBase for different error scenarios.

Exception Hierarchy
-----------------

ProtoBase defines a hierarchy of exception classes to handle different types of errors:

- ``ProtoBaseException``: The base class for all ProtoBase exceptions.
  - ``ProtoUserException``: Base class for exceptions that are expected to be handled by the user.
    - ``ProtoValidationException``: Raised when validation fails.
    - ``ProtoNotSupportedException``: Raised when an operation is not supported.
    - ``ProtoNotAuthorizedException``: Raised when an operation is not authorized.
  - ``ProtoCorruptionException``: Raised when data corruption is detected.
  - ``ProtoUnexpectedException``: Base class for unexpected exceptions.
    - ``CloudStorageError``: Raised for cloud storage specific errors.
    - ``CloudClusterStorageError``: Raised for cloud cluster storage specific errors.

Exception Classes
---------------

ProtoBaseException
~~~~~~~~~~~~~~~~

.. autoclass:: ProtoBaseException
   :members:
   :special-members: __init__

The base class for all ProtoBase exceptions. All other exception classes inherit from this class.

ProtoUserException
~~~~~~~~~~~~~~~~

.. autoclass:: ProtoUserException
   :members:
   :special-members: __init__

Base class for exceptions that are expected to be handled by the user. These exceptions indicate errors in the user's code or input.

ProtoValidationException
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ProtoValidationException
   :members:
   :special-members: __init__

Raised when validation fails. This can happen when invalid arguments are passed to a method or when an operation would result in an invalid state.

ProtoNotSupportedException
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ProtoNotSupportedException
   :members:
   :special-members: __init__

Raised when an operation is not supported. This can happen when trying to use a feature that is not implemented or not available in the current context.

ProtoNotAuthorizedException
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ProtoNotAuthorizedException
   :members:
   :special-members: __init__

Raised when an operation is not authorized. This can happen when trying to perform an operation that requires higher privileges.

ProtoCorruptionException
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ProtoCorruptionException
   :members:
   :special-members: __init__

Raised when data corruption is detected. This can happen when reading corrupted data from storage or when an internal consistency check fails.

ProtoUnexpectedException
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ProtoUnexpectedException
   :members:
   :special-members: __init__

Base class for unexpected exceptions. These exceptions indicate errors that are not expected to occur in normal operation and may indicate bugs in the code.

CloudStorageError
~~~~~~~~~~~~~~~

.. autoclass:: proto_db.cloud_file_storage.CloudStorageError
   :members:
   :special-members: __init__

Raised for cloud storage specific errors. This can happen when there are issues with S3 operations or when the cloud storage is misconfigured.

CloudClusterStorageError
~~~~~~~~~~~~~~~

.. autoclass:: proto_db.cloud_cluster_file_storage.CloudClusterStorageError
   :members:
   :special-members: __init__

Raised for cloud cluster storage specific errors. This can happen when there are issues with distributed operations in a cluster environment that uses S3 as the final storage for data.

Usage Examples
-------------

Handling Exceptions
~~~~~~~~~~~~~~~~~

.. code-block:: python

    import proto_db
    from proto_db.exceptions import ProtoBaseException, ProtoValidationException

    try:
        # Perform an operation that might raise an exception
        storage = proto_db.MemoryStorage()
        space = proto_db.ObjectSpace(storage)
        db = space.get_database("test_db")
        tr = db.new_transaction()

        # This will raise a ProtoValidationException if the key is not a string
        tr.set_root_object(123, "value")

        tr.commit()
    except ProtoValidationException as e:
        # Handle validation errors
        print(f"Validation error: {e}")
    except ProtoBaseException as e:
        # Handle other ProtoBase exceptions
        print(f"ProtoBase error: {e}")
    except Exception as e:
        # Handle unexpected exceptions
        print(f"Unexpected error: {e}")
    finally:
        # Clean up resources
        if 'storage' in locals():
            storage.close()

Raising Custom Exceptions
~~~~~~~~~~~~~~~~~~~~~~~

You can also define your own exception classes that inherit from the ProtoBase exception classes:

.. code-block:: python

    from proto_db.exceptions import ProtoUserException

    class MyCustomException(ProtoUserException):
        """
        A custom exception for my application.
        """
        def __init__(self, message, custom_info=None):
            super().__init__(message=message)
            self.custom_info = custom_info

    # Raise the custom exception
    raise MyCustomException("Something went wrong", custom_info="Additional information")
