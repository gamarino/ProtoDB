"""The registry module ``acme_sales`` as code without the semantic layer would declare it (Review Focus 1):
plain rc4 extensions, a data default and a method, no descriptor.  It is imported only by a process of its
own (``RC4_MAPPING``), since a process declares a registry module name once."""
from proto_db.registry import module

sales = module("acme_sales")


@sales.extends("acme.sales.Order")
class Order:
    state = "open"

    def describe_number(self):
        return f"order {self.number}"
