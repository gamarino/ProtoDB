Introduction
===========

What is ProtoBase?
-----------------

ProtoBase is a powerful database system implemented in Python, designed to provide flexible storage options and rich data structures. It offers a unique combination of features that make it suitable for a wide range of applications, from simple data storage to complex distributed systems.

Key Features
-----------

* **Multiple Storage Options**: Choose from in-memory storage, file-based storage, distributed cluster storage, or cloud-based storage.
* **Rich Data Structures**: Built-in support for dictionaries, lists, sets, and hash dictionaries.
* **Transactional Operations**: All database operations are performed within transactions, ensuring data consistency.
* **Query System**: Powerful query capabilities including filtering, joining, grouping, and sorting.
* **Distributed Capabilities**: Support for distributed operations across multiple nodes in a cluster.
* **Cloud Integration**: Seamless integration with cloud object storage services (Amazon S3 and Google Cloud Storage).

Use Cases
--------

ProtoBase is well-suited for a variety of use cases:

* **Embedded Databases**: Use the in-memory or file-based storage for applications that need a lightweight, embedded database.
* **Distributed Systems**: Leverage the cluster storage option for applications that require high availability and horizontal scaling.
* **Cloud-Native Applications**: Utilize the cloud storage option for applications deployed in cloud environments.
* **Data Analysis**: Take advantage of the rich query system for data analysis and reporting.
* **Real-time Collaborative Applications**: Use the distributed capabilities for applications that require real-time collaboration.

Architecture Overview
-------------------

ProtoBase is organized around several key abstractions:

* **Atom**: The base class for all database objects.
* **Storage Layer**: Responsible for persisting atoms to disk, memory, or cloud storage.
* **Data Structures**: Built on top of atoms to provide higher-level abstractions.
* **Query System**: Allows for complex data manipulation and retrieval.

For a more detailed explanation of the architecture, see the :doc:`architecture` section.
