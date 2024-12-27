import unittest
from uuid import uuid4
from ..common import Atom
from ..lists import List


class TestListOperations(unittest.TestCase):

    def setUp(self):
        """
        Inicializa un entorno limpio para cada prueba.
        """
        self.empty_list = List(empty=True)
        self.atom_a = Atom()  # Simula un objeto Atom
        self.atom_b = Atom()
        self.atom_c = Atom()

    # --- Test get_at ---
    def test_get_at(self):
        # Lista vacía
        self.assertIsNone(self.empty_list.get_at(0), "Should return None for an empty list.")

        # Lista con nodos
        node1 = List(value=self.atom_a)
        node2 = List(value=self.atom_b, previous=node1)
        node1.next = node2
        node3 = List(value=self.atom_c, previous=node2)
        node2.next = node3

        self.assertEqual(node1.get_at(0), self.atom_a, "Should return the correct value at offset 0.")
        self.assertEqual(node1.get_at(2), self.atom_c, "Should return the correct value at offset 2.")
        self.assertIsNone(node1.get_at(3), "Should return None for an out-of-bounds offset.")

    # --- Test set_at ---
    def test_set_at(self):
        # Insertar en una lista vacía
        updated_list = self.empty_list.set_at(0, self.atom_a)
        self.assertEqual(updated_list.get_at(0), self.atom_a, "Should insert value at position 0 in empty list.")

        # Crear un nodo y actualizar su posición
        node1 = List(value=self.atom_a)
        updated_node1 = node1.set_at(0, self.atom_b)
        self.assertEqual(updated_node1.get_at(0), self.atom_b, "Should update node at given position.")

    # --- Test insert_at ---
    def test_insert_at(self):
        # Insertar en una lista vacía
        updated_list = self.empty_list.insert_at(0, self.atom_a)
        self.assertEqual(updated_list.get_at(0), self.atom_a, "Should insert value in empty list.")

        # Insertar al principio y al final
        updated_list = updated_list.insert_at(0, self.atom_b)  # Nuevo al inicio
        self.assertEqual(updated_list.get_at(0), self.atom_b, "Should insert value at the start.")

        updated_list = updated_list.insert_at(-1, self.atom_c)  # Nuevo al final
        self.assertEqual(updated_list.get_at(2), self.atom_c, "Should insert value at the end.")

    # --- Test remove_at ---
    def test_remove_at(self):
        # Eliminar de una lista vacía
        updated_list = self.empty_list.remove_at(0)
        self.assertTrue(updated_list.empty, "Should remain empty after removing from an empty list.")

        # Crear lista y eliminar elementos
        node1 = List(value=self.atom_a)
        node2 = List(value=self.atom_b, previous=node1)
        node1.next = node2
        node3 = List(value=self.atom_c, previous=node2)
        node2.next = node3

        # Eliminar el primer nodo
        updated_list = node1.remove_at(0)
        self.assertEqual(updated_list.get_at(0), self.atom_b, "Should remove the first element.")

        # Eliminar el último nodo
        updated_list = updated_list.remove_at(1)
        self.assertEqual(updated_list.get_at(0), self.atom_b, "Should remove the last element.")

    # --- Test slice ---
    def test_slice(self):
        # Cortar una lista vacía
        sliced = self.empty_list.slice(0, 1)
        self.assertTrue(sliced.empty, "Should return an empty list when slicing an empty list.")

        # Crear una lista simple y realizar un corte
        node1 = List(value=self.atom_a)
        node2 = List(value=self.atom_b, previous=node1)
        node1.next = node2
        node3 = List(value=self.atom_c, previous=node2)
        node2.next = node3

        sliced = node1.slice(0, 2)
        self.assertEqual(sliced.get_at(0), self.atom_a, "Should slice and contain the first element.")
        self.assertEqual(sliced.get_at(1), self.atom_b, "Should slice and contain the second element.")

    # --- Test _rebalance ---
    def test_rebalance(self):
        # Nodo balanceado
        root = List(value=10)
        left = List(value=5)
        right = List(value=15)
        root.previous = left
        root.next = right

        balanced = root._rebalance()
        self.assertEqual(balanced.value, 10, "Balanced node should remain unchanged.")

        # Desbalance: rotación derecha
        unbalanced = List(value=10, previous=List(value=5, previous=List(value=2)))
        rebalanced = unbalanced._rebalance()
        self.assertEqual(rebalanced.value, 5, "Should perform correct rotations.")

    # --- Test as_iterable ---
    def test_as_iterable(self):
        self.assertEqual(self.empty_list.as_iterable(), [], "Empty list should return an empty iterable.")

        # Crear nodos
        node1 = List(value=self.atom_a)
        node2 = List(value=self.atom_b, previous=node1)
        node1.next = node2
        node3 = List(value=self.atom_c, previous=node2)
        node2.next = node3

        result = node1.as_iterable()
        self.assertEqual(len(result), 3, "Should contain all elements in the list.")
        self.assertEqual(result[0], (0, self.atom_a), "First element should match.")

    # --- Test append_first y append_last ---
    def test_append(self):
        # Agregar al principio
        updated_list = self.empty_list.append_first(self.atom_a)
        self.assertEqual(updated_list.get_at(0), self.atom_a, "Should insert value at the start.")

        # Agregar al final
        updated_list = updated_list.append_last(self.atom_b)
        self.assertEqual(updated_list.get_at(1), self.atom_b, "Should insert value at the end.")

    # --- Test extend ---
    def test_extend(self):
        # Extender una lista vacía
        list1 = List(value=self.atom_a)
        result = self.empty_list.extend(list1)
        self.assertEqual(result.get_at(0), self.atom_a, "Should extend the empty list with new items.")

        # Extender una lista no vacía
        list2 = List(value=self.atom_b)
        extended = list1.extend(list2)
        self.assertEqual(extended.get_at(1), self.atom_b, "Second element should be from the extended list.")


if __name__ == "__main__":
    unittest.main()
