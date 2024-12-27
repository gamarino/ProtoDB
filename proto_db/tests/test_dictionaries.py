import unittest
import uuid
from ..dictionaries import HashDictionary, Dictionary, DictionaryItem, Atom  # Sustituye 'your_module' por el nombre del módulo correcto


class TestHashDictionary(unittest.TestCase):

    def setUp(self):
        """
        Configuración inicial para las pruebas.
        """
        self.empty_dictionary = HashDictionary()  # Diccionario vacío
        self.atom_a = Atom()  # Simula un Atom como valor de prueba
        self.atom_b = Atom()
        self.atom_c = Atom()

    # --- Test set_at ---
    def test_set_at(self):
        # Insertar en un diccionario vacío
        updated_dict = self.empty_dictionary.set_at(10, self.atom_a)
        self.assertEqual(updated_dict.get_at(10), self.atom_a, "Should insert value at key 10.")

        # Insertar un valor que requiere rebalanceo
        updated_dict = updated_dict.set_at(5, self.atom_b)
        self.assertEqual(updated_dict.get_at(5), self.atom_b, "Should insert value at key 5.")

        # Insertar un valor que ya existe
        updated_dict = updated_dict.set_at(10, self.atom_c)
        self.assertEqual(updated_dict.get_at(10), self.atom_c, "Should update the value of an existing key.")

    # --- Test get_at ---
    def test_get_at(self):
        # Diccionario vacío
        self.assertIsNone(self.empty_dictionary.get_at(10), "Should return None for an empty dictionary.")

        # Insertar nodo y comprobar búsqueda
        updated_dict = self.empty_dictionary.set_at(10, self.atom_a)
        self.assertEqual(updated_dict.get_at(10), self.atom_a, "Should return the correct value for the key.")

        # Búsqueda de clave inexistente
        self.assertIsNone(updated_dict.get_at(20), "Should return None for a non-existent key.")

    # --- Test _rebalance ---
    def test_rebalance(self):
        # Nodo balanceado
        root = HashDictionary(key=10, value=self.atom_a)
        left = HashDictionary(key=5, value=self.atom_b)
        right = HashDictionary(key=15, value=self.atom_c)
        root.previous = left
        root.next = right

        balanced = root._rebalance()
        self.assertEqual(balanced.key, 10, "Balanced tree should not change the root.")

        # Desbalance (rotación derecha)
        unbalanced = HashDictionary(key=10, previous=HashDictionary(key=5, previous=HashDictionary(key=2)))
        rebalanced = unbalanced._rebalance()
        self.assertEqual(rebalanced.key, 5, "Root should change after right rotation.")

        # Desbalance (rotación izquierda)
        unbalanced = HashDictionary(key=10, next=HashDictionary(key=15, next=HashDictionary(key=20)))
        rebalanced = unbalanced._rebalance()
        self.assertEqual(rebalanced.key, 15, "Root should change after left rotation.")

    # --- Test remove_key ---
    def test_remove_key(self):
        content = self.empty_dictionary.set_at(10, self.atom_a)
        content = content.set_at(20, self.atom_b)
        content = content.set_at(15, self.atom_c)

        dictionary = Dictionary(content=content)
        updated_dict = dictionary.remove_key("20")
        self.assertIsNone(updated_dict.content.get_at(20), "Should remove the key 20 from the content.")

    # --- Test as_iterable ---
    def test_as_iterable(self):
        content = self.empty_dictionary.set_at(10, self.atom_a)
        content = content.set_at(20, self.atom_b)
        content = content.set_at(15, self.atom_c)

        result = content.as_iterable()
        self.assertEqual(len(result), 3, "Should include all inserted keys in the iterable.")
        self.assertIn((10, self.atom_a), result, "Should include the correct key-value pair.")

    # --- Test as_query_plan ---
    def test_as_query_plan(self):
        content = self.empty_dictionary.set_at(10, self.atom_a)
        query_plan = content.as_query_plan()
        self.assertEqual(query_plan.base, content, "The query plan should reference the original content.")

    # --- Test Dictionary methods ---
    def test_dictionary_methods(self):
        # Crear un diccionario vacío
        dictionary = Dictionary(content=self.empty_dictionary)

        # Agregar valores
        dictionary = dictionary.set_at("key1", self.atom_a)
        dictionary = dictionary.set_at("key2", self.atom_b)
        self.assertEqual(
            dictionary.get_at("key1"),
            self.atom_a,
            "Should return the correct value for key 'key1'."
        )

        # Comprobar si tiene claves
        self.assertTrue(dictionary.has("key1"), "Should confirm the presence of 'key1'.")
        self.assertFalse(dictionary.has("keyX"), "Should confirm 'keyX' is not present.")

        # Eliminar una clave
        dictionary = dictionary.remove_key("key1")
        self.assertFalse(dictionary.has("key1"), "Should confirm 'key1' is removed.")
