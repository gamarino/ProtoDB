The complete tutorial
=====================

The complete tutorial is the place to start: one progressive tutorial, in eleven chapters, that builds a
modelled ERP (users, groups, persons and opportunities; a model, not an ERP product) from a first database to
a cluster. Its headline chapter is concurrency: the obvious approach, where it fails, and the precondition
model that replaces it, measured unit by unit. Two in-depth tutorials continue it on the same ERP, one on
queries and one on the semantic model.

The three tutorials are Markdown, kept in the repository next to the code they run:

========================================  ==================================================================
Tutorial                                  Where
========================================  ==================================================================
The complete tutorial (11 chapters)       ``docs/tutorial/README.md``
The queries tutorial (7 chapters)         ``docs/tutorial/queries/README.md``
The semantic model tutorial (7 chapters)  ``docs/tutorial/semantic-model/README.md``
========================================  ==================================================================

They are also browsable online, in the repository at
`github.com/gamarino/ProtoDB/tree/master/docs/tutorial <https://github.com/gamarino/ProtoDB/tree/master/docs/tutorial>`_.

Like the pages of this section, every code block of every chapter runs in the test suite
(``tests/docs/test_markdown_tutorials.py``), and every output shown is the one the code printed. Each chapter
also exists as a self-checking script under ``examples/tutorial/``, which compares what it prints with what its
chapter shows::

   python examples/tutorial/01_first_database.py
   python examples/tutorial/queries/01_building_queries.py
   python examples/tutorial/semantic/01_declaring_a_model.py

The topic tutorials of this section go deeper on one subject each, and the chapters link to them where they
do.
